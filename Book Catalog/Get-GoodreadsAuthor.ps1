[CmdletBinding(DefaultParameterSetName='ByUrl')]
param(
    # Provide an author name (e.g., "Brandon Sanderson") OR a Goodreads author URL.
    [Parameter(ParameterSetName='ByAuthor', Mandatory=$true)]
    [string]$Author,

    [Parameter(ParameterSetName='ByUrl', Mandatory=$true)]
    [Alias('ListUrl')]  # Back-compat
    [string]$Url,

    [switch]$ShowProgress,
    [string]$OutCsv
)

# ── TLS for older PS ────────────────────────────────────────────────────
try { [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12 } catch {}

# ── helpers ─────────────────────────────────────────────────────────────
function Normalize-Url {
    param([Parameter(Mandatory)][string]$Url)
    $u = $Url.Trim() -replace ' ', '%20'
    if ($u -notmatch '^[a-z][a-z0-9+\-.]*://') { $u = 'https://' + $u.TrimStart('/') }
    $uri = $null
    if (-not [System.Uri]::TryCreate($u, [System.UriKind]::Absolute, [ref]$uri)) {
        throw "Bad URL after normalization: '$u'"
    }
    $uri.AbsoluteUri
}

function Get-Html {
    param([string]$Url,[int]$MaxRetry = 3)
    for ($i = 1; $i -le $MaxRetry; $i++) {
        try {
            $norm = Normalize-Url $Url
            Write-Verbose "GET $norm (try $i)"
            return Invoke-WebRequest -Uri $norm -UseBasicParsing `
                   -UserAgent "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PowerShellScraper/1.3" `
                   -MaximumRedirection 3 -ErrorAction Stop
        } catch {
            if ($i -eq $MaxRetry) {
                throw "Invoke-WebRequest failed for URL '$Url' (normalized: '$norm'): $($_.Exception.Message)"
            }
            Start-Sleep -Seconds ([math]::Pow(2,$i))  # 2,4,8 backoff
        }
    }
}

function Parse-Int { param([string]$s) ($s -replace '[^\d]','') -as [int] }

function Resolve-GoodreadsAuthorListBaseUrl {
    param(
        [string]$Author,   # optional
        [string]$Url       # optional: can be author/show or author/list
    )

    $id = $null

    if ($Url) {
        # Accept either /author/show/<id> or /author/list/<id>
        if ($Url -match 'goodreads\.com/author/(?:show|list)/(?<id>\d+)') {
            $id = $Matches['id']
        } else {
            throw "URL must be a Goodreads author 'show' or 'list' page."
        }
    } elseif ($Author) {
        # Lookup by name via search
        $q = [System.Net.WebUtility]::UrlEncode($Author)
        $searchUrl = "https://www.goodreads.com/search?q=$q&search_type=authors"
        $searchHtml = (Get-Html $searchUrl).Content
        $m = [regex]::Match($searchHtml, '/author/(?:list|show)/(?<id>\d+)', 'IgnoreCase')
        if (!$m.Success) { throw "Could not find an author ID for '$Author'." }
        $id = $m.Groups['id'].Value
    } else {
        throw "Provide either -Author or -Url."
    }

    # Build a simple, unambiguous format template with literal {0}
    $baseNoQuery = "https://www.goodreads.com/author/list/$id"
    $template    = ('{0}?page={{0}}' -f $baseNoQuery)  # -> "https://.../list/<id>?page={0}"
    Write-Verbose "Resolved base list URL template: $template"
    Write-Verbose ("First page URL will be: {0}" -f ($template -f 1))
    return $template
}

# ── build base URL (accept Author name OR URL) ──────────────────────────
$baseUrl = $null
try {
    if ($PSCmdlet.ParameterSetName -eq 'ByAuthor') {
        $baseUrl = Resolve-GoodreadsAuthorListBaseUrl -Author $Author
    } else {
        $baseUrl = Resolve-GoodreadsAuthorListBaseUrl -Url $Url
    }
} catch {
    Write-Error $_.Exception.Message
    return
}

# ── scrape loop ─────────────────────────────────────────────────────────
$page      = 1
$books     = New-Object System.Collections.Generic.List[object]
$firstHtml = (Get-Html ($baseUrl -f $page)).Content
$totalPages= if ($firstHtml -match 'page\s+\d+\s+of\s+(\d+)'){[int]$matches[1]}else{$null}

if ($ShowProgress){
    Write-Progress -Id 1 -Activity "Scraping" -Status "Start" -PercentComplete 0
}
function Update-Bar { param($cur,$tot) if($ShowProgress){
    $pct = if($tot){[int](($cur-1)/$tot*100)}else{0}
    Write-Progress -Id 1 -Activity "Scraping" `
                   -Status "Page $cur$('/'+$tot)" -PercentComplete $pct
}}

do {
    Update-Bar $page $totalPages
    $html = if ($page -eq 1){$firstHtml}else{(Get-Html ($baseUrl -f $page)).Content}

    foreach ($row in ($html -split '(?=<tr)')) {

        if ($row -notmatch 'class="bookTitle"') { continue }

        # Skip Young-Adult rows
        if ($row -match '(?i)\bYoung Adult\b|\bYA\b') { continue }

        # Title
        $title = ''
        if ($row -match 'class="bookTitle"[^>]*>\s*(?:<span[^>]*>)?([^<]+)') {
            $title = [System.Net.WebUtility]::HtmlDecode($matches[1].Trim())
        }

        # Avg rating
        $rating = 0
        if     ($row -match '([\d.]+)\s*avg rating')                     { $rating = [double]$matches[1] }
        elseif ($row -match 'minirating"[^>]*>\s*([\d.]+)')             { $rating = [double]$matches[1] }
        elseif ($row -match 'itemprop="?ratingValue"?[^>]*>\s*([\d.]+)'){ $rating = [double]$matches[1] }

        # Reviews / ratings (population proxy)
        $reviews = 0
        if ($row -match '([\d,]+)\s*(?:reviews|ratings)') { $reviews = Parse-Int $matches[1] }

        # Publication year
        $pubYear = $null
        if ($row -match 'published\s+(?:\w+\s+)?(\d{4})') { $pubYear = [int]$matches[1] }

        # Series info – capture leading numeric part only
        $seriesName,$seriesNum = $null,$null
        $m=[regex]::Match($row,'\(([^#(]+)#\s*([\d]+(?:\.\d+)?)')
        if ($m.Success) {
            $seriesName = ($m.Groups[1].Value -replace '\s+$','').Trim()
            $numString  = $m.Groups[2].Value
            $tmp        = 0.0
            [double]::TryParse($numString,[System.Globalization.NumberStyles]::Float,
                               [System.Globalization.CultureInfo]::InvariantCulture,[ref]$tmp) | Out-Null
            if (-not [double]::IsNaN($tmp)) { $seriesNum = $tmp }
        }

        # Coarse NF flag (we still report Category for reference)
        $isNF = $row -match '(?i)Non[- ]?fiction|Memoir|Biography|Essays|True Crime'

        # Thresholds (≥4.0 rating; ≥1,000 for fiction; ≥10,000 for NF)
        $qualifies = $rating -ge 4 -and (
                        ($isNF   -and $reviews -ge 10000) -or
                        (-not $isNF -and $reviews -ge 1000)
                     )

        if ($qualifies) {
            $books.Add([pscustomobject]@{
                Title       = $title
                Category    = if ($isNF) { 'Non-fiction' } else { 'Fiction' }
                AvgRating   = $rating
                ReviewCount = $reviews
                PubYear     = $pubYear
                SeriesName  = $seriesName         # null for stand-alone
                SeriesNum   = $seriesNum          # null for stand-alone or unknown
            })
        }
    }

    $hasNext = $html -match 'rel="next"'
    $page++
    Start-Sleep -Milliseconds (Get-Random -Min 800 -Max 1600)
} while ($hasNext)

if ($ShowProgress){Write-Progress -Id 1 -Activity "Scraping" -Completed}

# ── compute earliest year per block (series or stand-alone) ─────────────
$firstYear = @{}
foreach ($b in $books) {
    $key = if ($b.SeriesName) { $b.SeriesName } else { $b.Title }
    if (-not $firstYear.ContainsKey($key) -or $firstYear[$key] -gt $b.PubYear) {
        $firstYear[$key] = $b.PubYear
    }
}
foreach ($b in $books) {
    $key = if ($b.SeriesName) { $b.SeriesName } else { $b.Title }
    $b | Add-Member -NotePropertyName BlockStartYear -NotePropertyValue $firstYear[$key]
    if ($b.SeriesNum -eq $null) { $b | Add-Member -Force SeriesNum ([double]::PositiveInfinity) }
}

# ── final multi-sort: BlockStartYear ➜ SeriesName/Title ➜ SeriesNum ➜ Title ──
$sorted = $books |
Sort-Object `
    @{Expression = 'BlockStartYear'; Ascending = $true}, `
    @{Expression = { if ($_.SeriesName) { $_.SeriesName } else { $_.Title } }; Ascending = $true}, `
    @{Expression = 'SeriesNum' ; Ascending = $true}, `
    @{Expression = 'Title'     ; Ascending = $true}

# ── table rows (formatted for console) ──────────────────────────────────
$tableRows = $sorted |
Select-Object `
    @{Label='Title'      ; Expression = { $_.Title }}, `
    @{Label='SeriesName' ; Expression = { $_.SeriesName }}, `
    @{Label='SeriesNum'  ; Expression = { if ([double]::IsInfinity($_.SeriesNum)) { $null } else { $_.SeriesNum } }}, `
    @{Label='PubYear'    ; Expression = { $_.PubYear }}, `
    @{Label='AvgRating'  ; Expression = { '{0:N2}' -f [double]$_.AvgRating }}, `
    @{Label='ReviewCount'; Expression = { '{0:N0}' -f [int]$_.ReviewCount }}

# ── CSV rows (clean numeric values) ─────────────────────────────────────
$csvRows = $sorted |
Select-Object `
    @{Name='Title'      ; Expression = { $_.Title }}, `
    @{Name='SeriesName' ; Expression = { $_.SeriesName }}, `
    @{Name='SeriesNum'  ; Expression = { if ([double]::IsInfinity($_.SeriesNum) -or $null -eq $_.SeriesNum) { $null } else { $_.SeriesNum } }}, `
    @{Name='PubYear'    ; Expression = { $_.PubYear }}, `
    @{Name='AvgRating'  ; Expression = { [math]::Round([double]$_.AvgRating,2) }}, `
    @{Name='ReviewCount'; Expression = { [int]$_.ReviewCount }}

# ── output table ────────────────────────────────────────────────────────
$tableRows | Format-Table -AutoSize -Wrap

# ── optional CSV export ─────────────────────────────────────────────────
if ($OutCsv) {
    try {
        $csvRows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutCsv
        Write-Host "Saved CSV to: $OutCsv"
    } catch {
        Write-Warning "Failed to write CSV: $($_.Exception.Message)"
    }
}
