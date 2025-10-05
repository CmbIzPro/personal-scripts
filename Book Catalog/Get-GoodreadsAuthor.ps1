[CmdletBinding()]
param(
    # You can pass one or more author names and/or one or more Goodreads author URLs,
    # OR provide a CSV with columns like Author, Authors, Name, Url, URL, ListUrl, AuthorUrl.
    [string[]]$Author,
    [Alias('ListUrl')]
    [string[]]$Url,
    [string]$InCsv,

    [switch]$ShowProgress,
    [string]$OutCsv
)

# ── TLS for older PS ────────────────────────────────────────────────────
try { [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12 } catch {}

# ── sanity check ────────────────────────────────────────────────────────
if ((-not $Author -or $Author.Count -eq 0) -and (-not $Url -or $Url.Count -eq 0) -and (-not $InCsv)) {
    throw "Provide one or more -Author values and/or -Url values, or specify -InCsv with a CSV file."
}

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
} # end Normalize-Url

function Strip-Tags {
    param([string]$Html)
    if (-not $Html) { return $Html }
    return ([regex]::Replace($Html, '<[^>]+>', '')).Trim()
} # end Strip-Tags

function Get-Html {
    param([string]$Url,[int]$MaxRetry = 3)
    for ($i = 1; $i -le $MaxRetry; $i++) {
        try {
            $norm = Normalize-Url $Url
            Write-Verbose "GET $norm (try $i)"
            return Invoke-WebRequest -Uri $norm -UseBasicParsing `
                   -UserAgent "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PowerShellScraper/1.8" `
                   -MaximumRedirection 5 -ErrorAction Stop
        } catch {
            if ($i -eq $MaxRetry) {
                throw "Invoke-WebRequest failed for URL '$Url' (normalized: '$norm'): $($_.Exception.Message)"
            }
            Start-Sleep -Seconds ([math]::Pow(2,$i))
        }
    }
} # end Get-Html

function Parse-Int { param([string]$s) ($s -replace '[^\d]','') -as [int] } # end Parse-Int

function Resolve-GoodreadsAuthorListBaseUrl {
    param(
        [string]$Author,   # optional
        [string]$Url       # optional: can be author/show or author/list
    )

    $id = $null

    if ($Url) {
        if ($Url -match 'goodreads\.com/author/(?:show|list)/(?<id>\d+)') {
            $id = $Matches['id']
        } else {
            throw "URL must be a Goodreads author 'show' or 'list' page."
        }
    } elseif ($Author) {
        $q = [System.Net.WebUtility]::UrlEncode($Author)
        $searchUrl = "https://www.goodreads.com/search?q=$q&search_type=authors"
        $searchHtml = (Get-Html $searchUrl).Content
        $m = [regex]::Match($searchHtml, '/author/(?:list|show)/(?<id>\d+)', 'IgnoreCase')
        if (!$m.Success) { throw "Could not find an author ID for '$Author'." }
        $id = $m.Groups['id'].Value
    } else {
        throw "Provide either -Author or -Url to resolve the author list base URL."
    }

    $baseNoQuery = "https://www.goodreads.com/author/list/$id"
    $template    = ('{0}?page={{0}}' -f $baseNoQuery)  # -> "https://.../list/<id>?page={0}"
    return $template
} # end Resolve-GoodreadsAuthorListBaseUrl

function Get-AuthorIdFromListTemplate {
    param([string]$Template)
    $m = [regex]::Match($Template,'/author/list/(?<id>\d+)','IgnoreCase')
    if ($m.Success) { return $m.Groups['id'].Value }
    return $null
} # end Get-AuthorIdFromListTemplate

function Get-AuthorDisplayNameById {
    param([Parameter(Mandatory)][string]$AuthorId)

    $showUrl = "https://www.goodreads.com/author/show/$AuthorId"
    try {
        $html = (Get-Html $showUrl).Content
    } catch {
        return "Author $AuthorId"
    }

    # JSON-LD Person
    $scripts = [regex]::Matches($html,'<script[^>]+type="application/ld\+json"[^>]*>(?<j>[\s\S]+?)</script>','IgnoreCase')
    foreach ($s in $scripts) {
        $j = $s.Groups['j'].Value
        if ($j -match '"@type"\s*:\s*"Person"') {
            $m = [regex]::Match($j, '"name"\s*:\s*"(?<nm>[^"]+)"', 'IgnoreCase')
            if ($m.Success) { return [System.Net.WebUtility]::HtmlDecode($m.Groups['nm'].Value).Trim() }
        }
    }

    # Fallbacks
    $m = [regex]::Match($html,'<h1[^>]*class="authorName"[^>]*>[\s\S]*?<span[^>]*itemprop="name"[^>]*>(?<nm>[^<]+)</span>','IgnoreCase')
    if ($m.Success) { return [System.Net.WebUtility]::HtmlDecode($m.Groups['nm'].Value).Trim() }

    $m = [regex]::Match($html,'data-testid="authorName"[^>]*>\s*([^<]+)\s*<','IgnoreCase')
    if ($m.Success) { return [System.Net.WebUtility]::HtmlDecode($m.Groups[1].Value).Trim() }

    return "Author $AuthorId"
} # end Get-AuthorDisplayNameById

# Extract number of pages from a Goodreads book page (scan JSON-LD first, then fallbacks)
function Get-PageCountFromHtml {
    param([Parameter(Mandatory)][string]$Html)

    # 1) JSON-LD: numberOfPages
    $jsonMatches = [regex]::Matches($Html, '<script[^>]+type="application/ld\+json"[^>]*>(?<j>[\s\S]+?)</script>', 'IgnoreCase')
    foreach ($jm in $jsonMatches) {
        $j = $jm.Groups['j'].Value
        if ($j -match '"@type"\s*:\s*"Book"') {
            $n = [regex]::Match($j, '"numberOfPages"\s*:\s*"?(?<p>\d{1,5})"?', 'IgnoreCase')
            if ($n.Success) { return [int]$n.Groups['p'].Value }
        }
    }

    # 2) Structured markup near details/panels
    $region = $Html
    $anchor = [regex]::Match($Html, '(?i)>\s*Book\s*Details\s*<|>\s*Book\s*details\s*<|data-testid="bookDetails"|data-testid="pagesFormat"')
    if ($anchor.Success) {
        $start = [Math]::Max(0, $anchor.Index - 1200)
        $len   = [Math]::Min(30000, $Html.Length - $start)
        $region = $Html.Substring($start, $len)
    }

    $m = [regex]::Match($region, '<meta[^>]+itemprop="numberOfPages"[^>]+content="(?<p>\d{1,5})"', 'IgnoreCase')
    if ($m.Success) { return [int]$m.Groups['p'].Value }

    $m = [regex]::Match($region, '<span[^>]*itemprop="numberOfPages"[^>]*>\s*(?<p>\d{1,5})\s*pages?\s*</span>', 'IgnoreCase')
    if ($m.Success) { return [int]$m.Groups['p'].Value }

    $m = [regex]::Match($region, 'data-testid="pagesFormat"[^>]*>[\s\S]{0,800}?(?<p>\d{1,5})\s*pages', 'IgnoreCase,Singleline')
    if ($m.Success) { return [int]$m.Groups['p'].Value }

    # 3) Plain-text fallback anywhere
    $m = [regex]::Match($Html, '(?<!\d)(?<p>\d{1,5})\s*pages\b', 'IgnoreCase')
    if ($m.Success) { return [int]$m.Groups['p'].Value }

    return $null
} # end Get-PageCountFromHtml

# Parse a single Goodreads book page to get Title, PubYear, Genres (for YA/MG check), and Pages
function Get-BookPageDetails {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$BookUrl)

    $html = (Get-Html $BookUrl).Content

    # If we landed on a /work/, hop to canonical /book/show/
    if ($BookUrl -match '/work/') {
        $canon = [regex]::Match($html, '<link[^>]+rel="canonical"[^>]+href="(?<h>[^"]+)"','IgnoreCase')
        if ($canon.Success -and $canon.Groups['h'].Value -match '/book/show/') {
            $html = (Get-Html $canon.Groups['h'].Value).Content
        } else {
            $m = [regex]::Match($html, 'href="(?<h>/book/show/[^"#]+)"','IgnoreCase')
            if ($m.Success) { $html = (Get-Html ("https://www.goodreads.com" + $m.Groups['h'].Value)).Content }
        }
    }

    # --- Title: prefer JSON-LD or H1; if OG is exactly/starts with 'Goodreads', ignore it entirely ---
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
            $title = [System.Net.WebUtility]::HtmlDecode((Strip-Tags $mh1.Groups['t'].Value))
        }
    }
    if (-not $title) {
        $mog = [regex]::Match($html, '<meta[^>]+property="og:title"[^>]+content="(?<t>[^"]+)"', 'IgnoreCase')
        if ($mog.Success) {
            $cand = [System.Net.WebUtility]::HtmlDecode($mog.Groups['t'].Value).Trim()
            if ($cand -notmatch '^(?i)goodreads\b') { $title = $cand }
        }
    }

    # Pub year
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

    # Genres (for YA/MG filtering)
    $genres = New-Object System.Collections.Generic.List[string]
    $genrePatterns = @(
        '<a[^>]*class="[^"]*bookPageGenreLink[^"]*"[^>]*>(?<g>[^<]+)</a>',
        '<a[^>]*data-testid="bookPageGenreLink"[^>]*>(?<g>[^<]+)</a>',
        '<a[^>]*data-testid="genreChip"[^>]*>(?<g>[^<]+)</a>',
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

    # Focused snippet + big haystack (help catch all variants)
    $snippet = ''
    $blk = [regex]::Match($html, '(<section[^>]*genres[^>]*>[\s\S]{0,8000}?</section>)|(<div[^>]*genres[^>]*>[\s\S]{0,8000}?</div>)', 'IgnoreCase')
    if ($blk.Success) { $snippet = $blk.Value } else {
        $anchors = [regex]::Matches($html, '<a[^>]+href="/genres/[^"]+"[^>]*>[^<]+</a>', 'IgnoreCase')
        if ($anchors.Count -gt 0) {
            $sb = New-Object System.Text.StringBuilder
            foreach ($a in $anchors) { [void]$sb.Append($a.Value) }
            $snippet = $sb.ToString()
        }
    }

    $pages = Get-PageCountFromHtml -Html $html

    $hayLen = [Math]::Min($html.Length, 150000)
    $genresHaystack = ($snippet + ' ' + $html.Substring(0, $hayLen))

    return [pscustomobject]@{
        Title          = $title
        PubYear        = $pubYear
        Genres         = $genres
        GenresHtml     = $snippet
        GenresHaystack = $genresHaystack
        Pages          = $pages
    }
} # end Get-BookPageDetails

function Test-IsYAOrMiddleGrade {
    param(
        [string[]]$Genres,
        [string]$GenresHtml,
        [string]$FullHtml  # optional big haystack
    )

    if (-not $Genres)    { $Genres     = @() }
    if (-not $GenresHtml){ $GenresHtml = '' }
    if (-not $FullHtml)  { $FullHtml   = '' }

    # Word-based variants to check ONLY in extracted genre names (avoid scanning whole page text).
    # Keep this tight to avoid false positives.
    $wordPat = '(?i)\b(young[\s-]*adult|ya|middle[\s-]*grade|mg|teen(?:s)?|juvenile)\b'

    # Children's genre with apostrophes (children's / children’s), also ONLY in genre names.
    # Use regex escapes for apostrophes to avoid quoting issues.
    $childAposPat = '(?i)\bchild(?:ren)?(?:(?:\x27|\u2019))s\b'

    # Genre/shelf URL patterns: safe to use on the HTML haystacks.
    $hrefPat = '(?i)/(genres|shelf/show)/(young-adult|ya|middle-grade|children|childrens|kids|juvenile|teen)\b'

    # 1) Check explicit genre tags (strings)
    foreach ($g in $Genres) {
        if (-not $g) { continue }
        if ($g -match $wordPat)       { return $true }
        if ($g -match $childAposPat)  { return $true }  # "Children's", "Children’s"
        # NOTE: we intentionally do NOT match plain "children" or "kids" here to avoid false positives
    }

    # 2) Check focused genres HTML for explicit genre/shelf links
    if ($GenresHtml -match $hrefPat) { return $true }

    # 3) As a last resort, check the larger HTML only for explicit genre/shelf links (not free text)
    if ($FullHtml -match $hrefPat)   { return $true }

    return $false
}


function Get-BooksForAuthor {
    param(
        [Parameter(Mandatory)][string]$BaseTemplate,
        [Parameter(Mandatory)][string]$AuthorName,
        [switch]$ShowProgress
    )

    # ── scrape list pages (numeric prefilter) ────────────────────────────
    $page      = 1
    $books     = New-Object System.Collections.Generic.List[object]
    $firstHtml = (Get-Html ($BaseTemplate -f $page)).Content
    $totalPages= if ($firstHtml -match 'page\s+\d+\s+of\s+(\d+)'){[int]$matches[1]}else{$null}

    if ($ShowProgress){
        Write-Progress -Id 11 -Activity "Scraping list pages ($AuthorName)" -Status "Start" -PercentComplete 0
    }
    function Update-BarLocal { param($cur,$tot,$name,$show)
        if($show){
            $pct = if($tot){[int](($cur-1)/$tot*100)}else{0}
            Write-Progress -Id 11 -Activity "Scraping list pages ($name)" `
                           -Status "Page $cur$('/'+$tot)" -PercentComplete $pct
        }
    }

    do {
        Update-BarLocal $page $totalPages $AuthorName $ShowProgress
        $html = if ($page -eq 1){$firstHtml}else{(Get-Html ($BaseTemplate -f $page)).Content}

        foreach ($row in ($html -split '(?=<tr)')) {
            if ($row -notmatch 'class="bookTitle"') { continue }

            # --- Title + URL (robust across newlines & nested spans) ---
            $titleFromRow = ''
            $bookUrl = $null
            $mTitle = [regex]::Match($row,'<a[^>]*class="bookTitle"[^>]*href="(?<href>[^"]+)"[^>]*>(?<inner>[\s\S]*?)</a>','IgnoreCase,Singleline')
            if ($mTitle.Success) {
                $href = $mTitle.Groups['href'].Value
                $bookUrl = if ($href -like 'http*') { $href } else { "https://www.goodreads.com$href" }
                $inner = $mTitle.Groups['inner'].Value
                $mName = [regex]::Match($inner,'<span[^>]*itemprop="name"[^>]*>(?<t>[^<]+)</span>','IgnoreCase')
                if ($mName.Success) {
                    $titleFromRow = [System.Net.WebUtility]::HtmlDecode($mName.Groups['t'].Value).Trim()
                } else {
                    $titleFromRow = [System.Net.WebUtility]::HtmlDecode((Strip-Tags $inner))
                }
            } else { continue }

            # Coarse YA/MG skip (final check on book page)
            if ($row -match '(?i)\byoung[\s-]*adult\b|\bya\b|\bmiddle[\s-]*grade\b|\bmg\b|\bteen\b') { continue }

            # Avg rating
            $rating = 0
            if     ($row -match '([\d.]+)\s*avg rating')                     { $rating = [double]$matches[1] }
            elseif ($row -match 'minirating"[^>]*>\s*([\d.]+)')             { $rating = [double]$matches[1] }
            elseif ($row -match 'itemprop="?ratingValue"?[^>]*>\s*([\d.]+)'){ $rating = [double]$matches[1] }

            # Reviews / ratings count
            $reviews = 0
            if ($row -match '([\d,]+)\s*(?:reviews|ratings)') { $reviews = Parse-Int $matches[1] }

            # Publication year (row-level; may be missing)
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

            # Coarse NF flag (for info only)
            $isNF = $row -match '(?i)Non[- ]?fiction|Memoir|Biography|Essays|True Crime'

            # Numeric thresholds
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
                    PubYear     = $pubYear
                    SeriesName  = $seriesName
                    SeriesNum   = $seriesNum
                })
            }
        }

        $hasNext = $html -match 'rel="next"'
        $page++
        Start-Sleep -Milliseconds (Get-Random -Min 800 -Max 1600)
    } while ($hasNext)

    if ($ShowProgress){Write-Progress -Id 11 -Activity "Scraping list pages ($AuthorName)" -Completed}

    if (-not $books -or $books.Count -eq 0) {
        Write-Warning "No candidates found after numeric thresholds for '$AuthorName'."
        return @()
    }

    # ── verify: fetch book pages; apply YA/MG + missing year filters; get Pages ──
    $verified = New-Object System.Collections.Generic.List[object]
    $idx = 0
    foreach ($b in $books) {
        $idx++
        if ($ShowProgress) {
            $pct = [int](($idx / $books.Count) * 100)
            Write-Progress -Id 12 -Activity "Verifying book pages ($AuthorName)" -Status $b.TitleRow -PercentComplete $pct
        }
        try {
            $details = Get-BookPageDetails -BookUrl $b.Url

            # Prefer parsed title unless it's a generic 'Goodreads'; otherwise use row title
            $finalTitle = if ($details.Title -and $details.Title -notmatch '^(?i)goodreads\b') { $details.Title } else { $b.TitleRow }
            $year    = if ($b.PubYear) { $b.PubYear } else { $details.PubYear }

            if (-not $year) { continue }

            # YA/MG test with big haystack (also checks /shelf/show/)
            if (Test-IsYAOrMiddleGrade -Genres $details.Genres -GenresHtml $details.GenresHtml -FullHtml $details.GenresHaystack) { continue }

            $verified.Add([pscustomobject]@{
                Author       = $AuthorName
                Title        = $finalTitle
                Url          = $b.Url
                Category     = $b.Category
                AvgRating    = [math]::Round([double]$b.AvgRating,2)
                ReviewCount  = $b.ReviewCount
                PubYear      = $year
                SeriesName   = $b.SeriesName
                SeriesNum    = $b.SeriesNum
                Pages        = $details.Pages
            })
        } catch {
            # skip on fetch/parse failure
        }
        Start-Sleep -Milliseconds (Get-Random -Min 800 -Max 1600)
    }
    if ($ShowProgress) { Write-Progress -Id 12 -Activity "Verifying book pages ($AuthorName)" -Completed }

    if ($verified.Count -eq 0) { return @() }

    # ── compute earliest year per block (series or stand-alone), isolated per author ──
    $firstYear = @{}
    foreach ($b in $verified) {
        $key = if ($b.SeriesName) { "$AuthorName|$($b.SeriesName)" } else { "$AuthorName|$($b.Title)" }
        if (-not $firstYear.ContainsKey($key) -or $firstYear[$key] -gt $b.PubYear) {
            $firstYear[$key] = $b.PubYear
        }
    }
    foreach ($b in $verified) {
        $key = if ($b.SeriesName) { "$AuthorName|$($b.SeriesName)" } else { "$AuthorName|$($b.Title)" }
        $b | Add-Member -NotePropertyName BlockStartYear -NotePropertyValue $firstYear[$key]
        if ($b.SeriesNum -eq $null) { $b | Add-Member -Force SeriesNum ([double]::PositiveInfinity) }
    }

    return ,$verified
} # end Get-BooksForAuthor

# ── gather authors/urls from parameters and CSV ─────────────────────────
$authorsAll = @()
$urlsAll    = @()

if ($Author) { $authorsAll += $Author }
if ($Url)    { $urlsAll    += $Url    }

if ($InCsv) {
    try {
        $rows = Import-Csv -Path $InCsv
    } catch {
        Write-Error "Failed to read CSV '$InCsv': $($_.Exception.Message)"
        return
    }

    foreach ($row in $rows) {
        # Accept multiple possible header names and split by comma/semicolon inside a cell
        $authorFields = @('Author','Authors','Name')
        foreach ($f in $authorFields) {
            if ($row.PSObject.Properties.Name -contains $f -and $row.$f) {
                $authorsAll += ($row.$f -split '[,;]' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
            }
        }

        $urlFields = @('Url','URL','ListUrl','AuthorUrl','AuthorURL')
        foreach ($f in $urlFields) {
            if ($row.PSObject.Properties.Name -contains $f -and $row.$f) {
                $urlsAll += ($row.$f -split '[,;]' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
            }
        }
    }
}

# Deduplicate raw inputs
$authorsAll = $authorsAll | Where-Object { $_ } | Select-Object -Unique
$urlsAll    = $urlsAll    | Where-Object { $_ } | Select-Object -Unique

if (($authorsAll.Count -eq 0) -and ($urlsAll.Count -eq 0)) {
    Write-Warning "No authors or URLs found after parsing inputs."
    return
}

# ── build worklist (resolve templates, display names) ───────────────────
$work = New-Object System.Collections.Generic.List[pscustomobject]
$seenTemplates = @{}

foreach ($a in $authorsAll) {
    try {
        $tmpl = Resolve-GoodreadsAuthorListBaseUrl -Author $a
        if (-not $seenTemplates.ContainsKey($tmpl)) {
            $aid  = Get-AuthorIdFromListTemplate $tmpl
            $name = if ($aid) { Get-AuthorDisplayNameById -AuthorId $aid } else { $a }
            $work.Add([pscustomobject]@{ Template = $tmpl; AuthorId = $aid; AuthorName = $name })
            $seenTemplates[$tmpl] = $true
        }
    } catch {
        Write-Warning "Skipping author '$a': $($_.Exception.Message)"
    }
}
foreach ($u in $urlsAll) {
    try {
        $tmpl = Resolve-GoodreadsAuthorListBaseUrl -Url $u
        if (-not $seenTemplates.ContainsKey($tmpl)) {
            $aid  = Get-AuthorIdFromListTemplate $tmpl
            $name = if ($aid) { Get-AuthorDisplayNameById -AuthorId $aid } else { "Author from URL" }
            $work.Add([pscustomobject]@{ Template = $tmpl; AuthorId = $aid; AuthorName = $name })
            $seenTemplates[$tmpl] = $true
        }
    } catch {
        Write-Warning "Skipping URL '$u': $($_.Exception.Message)"
    }
}

if ($work.Count -eq 0) {
    Write-Warning "Nothing to process after resolving authors/urls."
    return
}

# ── scrape all requested authors ────────────────────────────────────────
$all = New-Object System.Collections.Generic.List[object]
foreach ($w in $work) {
    $items = Get-BooksForAuthor -BaseTemplate $w.Template -AuthorName $w.AuthorName -ShowProgress:$ShowProgress
    foreach ($it in $items) { [void]$all.Add($it) }
}

if ($all.Count -eq 0) {
    Write-Warning "No books met filters across all authors."
    return
}

# ── final multi-sort: Author ➜ BlockStartYear ➜ Series/Title ➜ SeriesNum ➜ Title ──
$sorted = $all |
Sort-Object `
    @{Expression = 'Author'       ; Ascending = $true}, `
    @{Expression = 'BlockStartYear'; Ascending = $true}, `
    @{Expression = { if ($_.SeriesName) { $_.SeriesName } else { $_.Title } }; Ascending = $true}, `
    @{Expression = 'SeriesNum'    ; Ascending = $true}, `
    @{Expression = 'Title'        ; Ascending = $true}

# ── table rows (console) ────────────────────────────────────────────────
$tableRows = $sorted |
Select-Object `
    @{Label='Author'     ; Expression = { $_.Author }}, `
    @{Label='Title'      ; Expression = { $_.Title }}, `
    @{Label='SeriesName' ; Expression = { $_.SeriesName }}, `
    @{Label='SeriesNum'  ; Expression = { if ([double]::IsInfinity($_.SeriesNum)) { $null } else { $_.SeriesNum } }}, `
    @{Label='PubYear'    ; Expression = { $_.PubYear }}, `
    @{Label='Pages'      ; Expression = { $_.Pages }}, `
    @{Label='AvgRating'  ; Expression = { '{0:N2}' -f [double]$_.AvgRating }}, `
    @{Label='ReviewCount'; Expression = { '{0:N0}' -f [int]$_.ReviewCount }}, `
    @{Label='Url'        ; Expression = { $_.Url }}

$tableRows | Format-Table -AutoSize -Wrap

# ── CSV rows (clean numeric values) ─────────────────────────────────────
$csvRows = $sorted |
Select-Object `
    @{Name='Author'     ; Expression = { $_.Author }}, `
    @{Name='Title'      ; Expression = { $_.Title }}, `
    @{Name='SeriesName' ; Expression = { $_.SeriesName }}, `
    @{Name='SeriesNum'  ; Expression = { if ([double]::IsInfinity($_.SeriesNum) -or $null -eq $_.SeriesNum) { $null } else { $_.SeriesNum } }}, `
    @{Name='PubYear'    ; Expression = { $_.PubYear }}, `
    @{Name='Pages'      ; Expression = { if ($_.Pages) { [int]$_.Pages } else { $null } }}, `
    @{Name='AvgRating'  ; Expression = { [math]::Round([double]$_.AvgRating,2) }}, `
    @{Name='ReviewCount'; Expression = { [int]$_.ReviewCount }}, `
    @{Name='Url'        ; Expression = { $_.Url }}

# ── optional CSV export ─────────────────────────────────────────────────
if ($OutCsv) {
    try {
        $csvRows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutCsv
        Write-Host "Saved CSV to: $OutCsv"
    } catch {
        Write-Warning "Failed to write CSV: $($_.Exception.Message)"
    }
}
