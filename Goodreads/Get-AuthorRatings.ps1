[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ListUrl,
    [switch]$ShowProgress
)

#── helpers ──────────────────────────────────────────────────────────────
function Get-Html {
    param([string]$Url,[int]$MaxRetry = 3)
    for ($i = 1; $i -le $MaxRetry; $i++) {
        try {
            Write-Verbose "GET $Url (try $i)"
            return Invoke-WebRequest -Uri $Url -UseBasicParsing `
                   -UserAgent "Mozilla/5.0" -MaximumRedirection 3 -ErrorAction Stop
        } catch {
            if ($i -eq $MaxRetry) { throw }
            Start-Sleep -Seconds ([math]::Pow(2,$i))
        }
    }
}
function Parse-Int { param([string]$s) ($s -replace '[^\d]','') -as [int] }

#── build base URL ───────────────────────────────────────────────────────
if ($ListUrl -notmatch '/author/list/\d+') {
    throw "ListUrl must be a Goodreads author/list page."
}
$baseUrl = $ListUrl.TrimEnd('/')
if ($baseUrl -match '(.*?)(\?|&)page=\d+') { $baseUrl = $matches[1] }
if ($baseUrl -match '\?') { $baseUrl += '&page={0}' } else { $baseUrl += '?page={0}' }

#── scrape loop ──────────────────────────────────────────────────────────
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

        # Skip Young-Adult
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

        # Reviews / ratings
        $reviews = 0
        if ($row -match '([\d,]+)\s*(?:reviews|ratings)') { $reviews = Parse-Int $matches[1] }

        # Publication year
        $pubYear = $null
        if ($row -match 'published\s+(?:\w+\s+)?(\d{4})') { $pubYear = [int]$matches[1] }

        # Series info  – capture leading numeric part only
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

        # Thresholds
        $isNF = $row -match '(?i)Nonfiction|Memoir|Biography'
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

#── compute earliest year per block (series or stand-alone) ─────────────
$firstYear = @{}   # key = SeriesName OR Title (for stand-alone) ➜ earliest year
foreach ($b in $books) {
    $key = if ($b.SeriesName) { $b.SeriesName } else { $b.Title }
    if (-not $firstYear.ContainsKey($key) -or $firstYear[$key] -gt $b.PubYear) {
        $firstYear[$key] = $b.PubYear
    }
}
foreach ($b in $books) {
    $key = if ($b.SeriesName) { $b.SeriesName } else { $b.Title }
    $b | Add-Member -NotePropertyName BlockStartYear -NotePropertyValue $firstYear[$key]
    # stand-alone books or missing SeriesNum => PositiveInfinity so they list after numbered parts
    if ($b.SeriesNum -eq $null) { $b | Add-Member -Force SeriesNum ([double]::PositiveInfinity) }
}

#── final multi-sort:  BlockStartYear ➜ SeriesName/Title ➜ SeriesNum ➜ Title ──
$books |
Sort-Object `
    @{Expression = 'BlockStartYear'; Ascending = $true}, `
    @{Expression = { if ($_.SeriesName) { $_.SeriesName } else { $_.Title } }; Ascending = $true}, `
    @{Expression = 'SeriesNum' ; Ascending = $true}, `
    @{Expression = 'Title'     ; Ascending = $true} |

#── TABLE OUTPUT (formatted columns) ─────────────────────────────────────
Select-Object `
    @{Label='Title'      ; Expression = { $_.Title }}, `
    @{Label='SeriesName' ; Expression = { $_.SeriesName }}, `
    @{Label='SeriesNum'  ; Expression = { if ([double]::IsInfinity($_.SeriesNum)) { $null } else { $_.SeriesNum } }}, `
    @{Label='PubYear'    ; Expression = { $_.PubYear }}, `
    @{Label='AvgRating'  ; Expression = { '{0:N2}' -f [double]$_.AvgRating }}, `
    @{Label='ReviewCount'; Expression = { '{0:N0}' -f [int]$_.ReviewCount }} |
Format-Table -AutoSize -Wrap
