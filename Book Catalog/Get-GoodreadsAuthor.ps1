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
                   -UserAgent "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PowerShellScraper/1.4" `
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

    # Simple, stable template with literal {0}
    $baseNoQuery = "https://www.goodreads.com/author/list/$id"
    $template    = ('{0}?page={{0}}' -f $baseNoQuery)  # -> "https://.../list/<id>?page={0}"
    Write-Verbose "Resolved base list URL template: $template"
    Write-Verbose ("First page URL will be: {0}" -f ($template -f 1))
    return $template
}

# Return Title, PubYear, Genres[], and a genres-only HTML snippet (for robust YA/MG checks)
function Get-BookPageDetails {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$BookUrl)

    $html = (Get-Html $BookUrl).Content

    # If /work/, hop to canonical /book/show/
    if ($BookUrl -match '/work/') {
        $canon = [regex]::Match($html, '<link[^>]+rel="canonical"[^>]+href="(?<h>[^"]+)"','IgnoreCase')
        if ($canon.Success -and $canon.Groups['h'].Value -match '/book/show/') {
            $html = (Get-Html $canon.Groups['h'].Value).Content
        } else {
            $m = [regex]::Match($html, 'href="(?<h>/book/show/[^"#]+)"','IgnoreCase')
            if ($m.Success) { $html = (Get-Html ("https://www.goodreads.com" + $m.Groups['h'].Value)).Content }
        }
    }

    # --- Title (JSON-LD or H1 or og:title) ---
    $title = $null
    $jsonld = [regex]::Matches($html, '<script[^>]+type="application/ld\+json"[^>]*>(?<j>[\s\S]+?)</script>', 'IgnoreCase')
    foreach ($s in $jsonld) {
        $j = $s.Groups['j'].Value
        if ($j -match '"@type"\s*:\s*"Book"') {
            $mt = [regex]::Match($j, '"name"\s*:\s*"(?<nm>[^"]+)"', 'IgnoreCase')
            if ($mt.Success) { $title = [System.Net.WebUtility]::HtmlDecode($mt.Groups['nm'].Value); break }
        }
    }
    if (-not $title) {
        $mh1 = [regex]::Match($html, '<h1[^>]*data-testid="bookTitle"[^>]*>(?<t>[\s\S]*?)</h1>', 'IgnoreCase')
        if ($mh1.Success) {
            $title = [regex]::Replace($mh1.Groups['t'].Value, '<.*?>', '')
            $title = [System.Net.WebUtility]::HtmlDecode($title).Trim()
        }
    }
    if (-not $title) {
        $mog = [regex]::Match($html, '<meta[^>]+property="og:title"[^>]+content="(?<t>[^"]+)"', 'IgnoreCase')
        if ($mog.Success) { $title = [System.Net.WebUtility]::HtmlDecode($mog.Groups['t'].Value).Trim() }
    }

    # --- Year detection ---
    $pubYear = $null
    foreach ($s in $jsonld) {
        $j = $s.Groups['j'].Value
        $my = [regex]::Match($j, '"datePublished"\s*:\s*"(?<d>[^"]+)"', 'IgnoreCase')
        if ($my.Success) {
            $y = [regex]::Match($my.Groups['d'].Value, '\b(\d{4})\b')
            if ($y.Success) { $pubYear = [int]$y.Groups[1].Value; break }
        }
    }
    if (-not $pubYear) {
        $m = [regex]::Match($html, '<meta[^>]+itemprop="datePublished"[^>]+content="(?<d>[^"]+)"', 'IgnoreCase')
        if ($m.Success) {
            $y = [regex]::Match($m.Groups['d'].Value, '\b(\d{4})\b')
            if ($y.Success) { $pubYear = [int]$y.Groups[1].Value }
        }
    }
    if (-not $pubYear) {
        $m = [regex]::Match($html, '(?:First\s+)?Published[^0-9]{0,30}(\d{4})', 'IgnoreCase')
        if ($m.Success) { $pubYear = [int]$m.Groups[1].Value }
    }

    # --- Genres + a focused genres HTML snippet ---
    $genres = New-Object System.Collections.Generic.List[string]
    $genrePatterns = @(
        '<a[^>]*class="[^"]*bookPageGenreLink[^"]*"[^>]*>(?<g>[^<]+)</a>',
        '<a[^>]*data-testid="bookPageGenreLink"[^>]*>(?<g>[^<]+)</a>',
        '<a[^>]*href="/genres/[^"]+"[^>]*>(?<g>[^<]+)</a>',
        '<a[^>]*class="[^"]*Button--tag-inline[^"]*"[^>]*>(?<g>[^<]+)</a>'
    )
    foreach ($pat in $genrePatterns) {
        $ms = [regex]::Matches($html, $pat, 'Singleline,IgnoreCase')
        foreach ($m in $ms) {
            $g = ([System.Net.WebUtility]::HtmlDecode($m.Groups['g'].Value)).Trim()
            if ($g -and -not $genres.Contains($g)) { [void]$genres.Add($g) }
        }
    }

    # Try to isolate a genres block/snippet to avoid matching reviews text
    $snippet = ''
    $blk = [regex]::Match($html, '(<section[^>]*genres[^>]*>[\s\S]{0,5000}?</section>)|(<div[^>]*genres[^>]*>[\s\S]{0,5000}?</div>)', 'IgnoreCase')
    if ($blk.Success) { $snippet = $blk.Value } else {
        # Fallback: keep only anchor tags to /genres/… areas
        $anchors = [regex]::Matches($html, '<a[^>]+href="/genres/[^"]+"[^>]*>[^<]+</a>', 'IgnoreCase')
        if ($anchors.Count -gt 0) {
            $sb = New-Object System.Text.StringBuilder
            foreach ($a in $anchors) { [void]$sb.Append($a.Value) }
            $snippet = $sb.ToString()
        }
    }

    return [pscustomobject]@{
        Title       = $title
        PubYear     = $pubYear
        Genres      = $genres
        GenresHtml  = $snippet
    }
}

function Test-IsYAOrMiddleGrade {
    param(
        [string[]]$Genres,
        [string]$GenresHtml
    )
    if (-not $Genres) { $Genres = @() }
    if (-not $GenresHtml) { $GenresHtml = '' }

    # Look for YA/MG anywhere (not just prefix), including common variants.
    # Use single quotes; escape the apostrophe by doubling it.
    $pattern = '(?<!\w)(young[\s-]*adult|ya|middle[\s-]*grade|mg|teen(?:s)?|juvenile|children(?:''s)?|childrens)(?!\w)'

    foreach ($g in $Genres) {
        if ($g -and ($g.ToLowerInvariant() -match $pattern)) { return $true }
    }

    # Also scan a focused genres HTML snippet (anchors to /genres/* etc.)
    if ($GenresHtml -match $pattern -or
        $GenresHtml -match '/genres/young-adult|/genres/middle-grade|/genres/children|/genres/childrens') {
        return $true
    }

    return $false
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

# ── scrape loop (collect candidates) ────────────────────────────────────
$page      = 1
$books     = New-Object System.Collections.Generic.List[object]
$firstHtml = (Get-Html ($baseUrl -f $page)).Content
$totalPages= if ($firstHtml -match 'page\s+\d+\s+of\s+(\d+)'){[int]$matches[1]}else{$null}

if ($ShowProgress){
    Write-Progress -Id 1 -Activity "Scraping list pages" -Status "Start" -PercentComplete 0
}
function Update-Bar { param($cur,$tot) if($ShowProgress){
    $pct = if($tot){[int](($cur-1)/$tot*100)}else{0}
    Write-Progress -Id 1 -Activity "Scraping list pages" `
                   -Status "Page $cur$('/'+$tot)" -PercentComplete $pct
}}

do {
    Update-Bar $page $totalPages
    $html = if ($page -eq 1){$firstHtml}else{(Get-Html ($baseUrl -f $page)).Content}

    foreach ($row in ($html -split '(?=<tr)')) {
        if ($row -notmatch 'class="bookTitle"') { continue }

        # Title + URL (/book/show/... or sometimes /work/...)
        $titleFromRow = ''
        $bookUrl = $null
        $mTitle = [regex]::Match($row,'class="bookTitle"[^>]*href="(?<href>[^"]+)"[^>]*>\s*(?:<span[^>]*>)?([^<]+)','IgnoreCase')
        if ($mTitle.Success) {
            $titleFromRow = [System.Net.WebUtility]::HtmlDecode($mTitle.Groups[2].Value.Trim())
            $href  = $mTitle.Groups['href'].Value
            $bookUrl = if ($href -like 'http*') { $href } else { "https://www.goodreads.com$href" }
        } else { continue }

        # Coarse skip (final check happens on book page)
        if ($row -match '(?i)\byoung[\s-]*adult\b|\bya\b|\bmiddle[\s-]*grade\b|\bmg\b|\bteen\b') { continue }

        # Avg rating
        $rating = 0
        if     ($row -match '([\d.]+)\s*avg rating')                     { $rating = [double]$matches[1] }
        elseif ($row -match 'minirating"[^>]*>\s*([\d.]+)')             { $rating = [double]$matches[1] }
        elseif ($row -match 'itemprop="?ratingValue"?[^>]*>\s*([\d.]+)'){ $rating = [double]$matches[1] }

        # Reviews / ratings count (population proxy)
        $reviews = 0
        if ($row -match '([\d,]+)\s*(?:reviews|ratings)') { $reviews = Parse-Int $matches[1] }

        # Publication year (row-level; may be missing and will be re-fetched)
        $pubYear = $null
        if ($row -match 'published\s+(?:\w+\s+)?(\d{4})') { $pubYear = [int]$matches[1] }

        # Series info
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

        # Coarse NF flag (for info only; thresholds kept as before)
        $isNF = $row -match '(?i)Non[- ]?fiction|Memoir|Biography|Essays|True Crime'

        # Numeric thresholds (fast pre-filter)
        $qualifies = $rating -ge 4 -and (
                        ($isNF   -and $reviews -ge 10000) -or
                        (-not $isNF -and $reviews -ge 1000)
                     )

        if ($qualifies) {
            $books.Add([pscustomobject]@{
                TitleRow    = $titleFromRow
                Url         = $bookUrl
                Category    = if ($isNF) { 'Non-fiction' } else { 'Fiction' }
                AvgRating   = $rating
                ReviewCount = $reviews
                PubYear     = $pubYear     # may be $null; we’ll fill from book page
                SeriesName  = $seriesName
                SeriesNum   = $seriesNum
            })
        }
    }

    $hasNext = $html -match 'rel="next"'
    $page++
    Start-Sleep -Milliseconds (Get-Random -Min 800 -Max 1600)
} while ($hasNext)

if ($ShowProgress){Write-Progress -Id 1 -Activity "Scraping list pages" -Completed}

if (-not $books -or $books.Count -eq 0) {
    Write-Warning "No candidates found after numeric thresholds."
    return
}

# ── verify: fetch book page for Title/Year/Genres; exclude YA/MG + missing year ──
$verified = New-Object System.Collections.Generic.List[object]
$idx = 0
foreach ($b in $books) {
    $idx++
    if ($ShowProgress) {
        Write-Progress -Id 2 -Activity "Verifying on book pages" -Status $b.TitleRow -PercentComplete ([int](($idx/$($books.Count))*100))
    }
    try {
        $details = Get-BookPageDetails -BookUrl $b.Url
        $finalTitle = if ($details.Title) { $details.Title } else { $b.TitleRow }
        $year    = if ($b.PubYear) { $b.PubYear } else { $details.PubYear }

        # Exclude if we cannot find a published year
        if (-not $year) { continue }

        # Exclude Young Adult / Middle Grade (and close equivalents)
        if (Test-IsYAOrMiddleGrade -Genres $details.Genres -GenresHtml $details.GenresHtml) { continue }

        $verified.Add([pscustomobject]@{
            Title        = $finalTitle
            Url          = $b.Url
            Category     = $b.Category
            AvgRating    = [math]::Round([double]$b.AvgRating,2)
            ReviewCount  = $b.ReviewCount
            PubYear      = $year
            SeriesName   = $b.SeriesName
            SeriesNum    = $b.SeriesNum
            Genres       = ($details.Genres -join '; ')
        })
    } catch {
        # Skip on fetch/parse failure
    }
    Start-Sleep -Milliseconds (Get-Random -Min 800 -Max 1600)
}
if ($ShowProgress) { Write-Progress -Id 2 -Activity "Verifying on book pages" -Completed }

if ($verified.Count -eq 0) {
    Write-Warning "After verification, no books remained (missing year or YA/Middle Grade filtered)."
    return
}

# ── compute earliest year per block (series or stand-alone) ─────────────
$firstYear = @{}
foreach ($b in $verified) {
    $key = if ($b.SeriesName) { $b.SeriesName } else { $b.Title }
    if (-not $firstYear.ContainsKey($key) -or $firstYear[$key] -gt $b.PubYear) {
        $firstYear[$key] = $b.PubYear
    }
}
foreach ($b in $verified) {
    $key = if ($b.SeriesName) { $b.SeriesName } else { $b.Title }
    $b | Add-Member -NotePropertyName BlockStartYear -NotePropertyValue $firstYear[$key]
    if ($b.SeriesNum -eq $null) { $b | Add-Member -Force SeriesNum ([double]::PositiveInfinity) }
}

# ── final multi-sort: BlockStartYear ➜ SeriesName/Title ➜ SeriesNum ➜ Title ──
$sorted = $verified |
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
    @{Label='ReviewCount'; Expression = { '{0:N0}' -f [int]$_.ReviewCount }}, `
    @{Label='Genres'     ; Expression = { $_.Genres }}, `
    @{Label='Url'        ; Expression = { $_.Url }}

# ── CSV rows (clean numeric values) ─────────────────────────────────────
$csvRows = $sorted |
Select-Object `
    @{Name='Title'      ; Expression = { $_.Title }}, `
    @{Name='SeriesName' ; Expression = { $_.SeriesName }}, `
    @{Name='SeriesNum'  ; Expression = { if ([double]::IsInfinity($_.SeriesNum) -or $null -eq $_.SeriesNum) { $null } else { $_.SeriesNum } }}, `
    @{Name='PubYear'    ; Expression = { $_.PubYear }}, `
    @{Name='AvgRating'  ; Expression = { [math]::Round([double]$_.AvgRating,2) }}, `
    @{Name='ReviewCount'; Expression = { [int]$_.ReviewCount }}, `
    @{Name='Genres'     ; Expression = { $_.Genres }}, `
    @{Name='Url'        ; Expression = { $_.Url }}

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
