[CmdletBinding()]
param(
    # You can pass one or more author names and/or one or more Goodreads author URLs,
    # OR provide a CSV with columns like:
    #   - Author/Authors/Name + Genre (or Genres/Category/Categories)
    #   - Url/URL/ListUrl/AuthorUrl + Genre (or Genres/Category/Categories)
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

# ── helpers (consolidated) ─────────────────────────────────────────────
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

function Strip-Tags { param([string]$Html) if (-not $Html) { return $Html } ; ([regex]::Replace($Html, '<[^>]+>', '')).Trim() }

function Clean-Text {
    param([string]$HtmlOrFragment)
    if (-not $HtmlOrFragment) { return $null }
    $t = [System.Net.WebUtility]::HtmlDecode(([regex]::Replace($HtmlOrFragment, '<[^>]+>', ' ')))
    $t = $t.Replace([char]0xA0, ' ')
    ($t -replace '\s+', ' ').Trim()
}

function Get-Html {
    param([string]$Url,[int]$MaxRetry = 3)
    $headers = @{
        'Accept'            = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        'Accept-Language'   = 'en-US,en;q=0.9'
        'Cache-Control'     = 'no-cache'
        'Pragma'            = 'no-cache'
    }
    for ($i = 1; $i -le $MaxRetry; $i++) {
        try {
            $norm = Normalize-Url $Url
            Write-Verbose "GET $norm (try $i)"
            return Invoke-WebRequest -Uri $norm -UseBasicParsing `
                   -UserAgent "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36" `
                   -Headers $headers -MaximumRedirection 5 -ErrorAction Stop
        } catch {
            if ($i -eq $MaxRetry) {
                throw "Invoke-WebRequest failed for URL '$Url' (normalized: '$norm'): $($_.Exception.Message)"
            }
            Start-Sleep -Seconds ([math]::Pow(2,$i))
        }
    }
}

function Parse-Int { param([string]$s) ($s -replace '[^\d]','') -as [int] }

function Get-JsonLdStrings {
    param([Parameter(Mandatory)][string]$Html)
    ([regex]::Matches($Html,'<script[^>]+type="application/ld\+json"[^>]*>(?<j>[\s\S]+?)</script>','IgnoreCase')).Groups |
        ForEach-Object { $_ } | Where-Object { $_.Name -eq 'j' } | ForEach-Object { $_.Value }
}

function Resolve-GoodreadsAuthorListBaseUrl {
    param([string]$Author,[string]$Url)
    $id = $null
    if ($Url) {
        if ($Url -match 'goodreads\.com/author/(?:show|list)/(?<id>\d+)') { $id = $Matches['id'] }
        else { throw "URL must be a Goodreads author 'show' or 'list' page." }
    } elseif ($Author) {
        $q = [System.Net.WebUtility]::UrlEncode($Author)
        $searchUrl = "https://www.goodreads.com/search?q=$q&search_type=authors"
        $searchHtml = (Get-Html $searchUrl).Content
        $m = [regex]::Match($searchHtml, '/author/(?:list|show)/(?<id>\d+)', 'IgnoreCase')
        if (!$m.Success) { throw "Could not find an author ID for '$Author'." }
        $id = $m.Groups['id'].Value
    } else { throw "Provide either -Author or -Url to resolve the author list base URL." }
    $baseNoQuery = "https://www.goodreads.com/author/list/$id"
    ('{0}?page={{0}}' -f $baseNoQuery)
}

function Get-AuthorIdFromListTemplate { param([string]$Template) $m = [regex]::Match($Template,'/author/list/(?<id>\d+)','IgnoreCase'); if ($m.Success) { $m.Groups['id'].Value } }

function Get-AuthorDisplayNameById {
    param([Parameter(Mandatory)][string]$AuthorId)
    $showUrl = "https://www.goodreads.com/author/show/$AuthorId"
    try { $html = (Get-Html $showUrl).Content } catch { return "Author $AuthorId" }
    foreach ($j in (Get-JsonLdStrings -Html $html)) {
        if ($j -match '"@type"\s*:\s*"Person"') {
            $m = [regex]::Match($j, '"name"\s*:\s*"(?<nm>[^"]+)"', 'IgnoreCase')
            if ($m.Success) { return [System.Net.WebUtility]::HtmlDecode($m.Groups['nm'].Value).Trim() }
        }
    }
    $m = [regex]::Match($html,'<h1[^>]*class="authorName"[^>]*>[\s\S]*?<span[^>]*itemprop="name"[^>]*>(?<nm>[^<]+)</span>','IgnoreCase')
    if ($m.Success) { return [System.Net.WebUtility]::HtmlDecode($m.Groups['nm'].Value).Trim() }
    $m = [regex]::Match($html,'data-testid="authorName"[^>]*>\s*([^<]+)\s*<','IgnoreCase')
    if ($m.Success) { return [System.Net.WebUtility]::HtmlDecode($m.Groups[1].Value).Trim() }
    "Author $AuthorId"
}

function Get-CanonicalBookHtml {
    param([Parameter(Mandatory)][string]$BookUrl)
    $html = (Get-Html $BookUrl).Content
    if ($BookUrl -match '/work/') {
        $canon = [regex]::Match($html, '<link[^>]+rel="canonical"[^>]+href="(?<h>[^"]+)"','IgnoreCase')
        if ($canon.Success -and $canon.Groups['h'].Value -match '/book/show/') { return (Get-Html $canon.Groups['h'].Value).Content }
        $m = [regex]::Match($html, 'href="(?<h>/book/show/[^"#]+)"','IgnoreCase')
        if ($m.Success) { return (Get-Html ("https://www.goodreads.com" + $m.Groups['h'].Value)).Content }
    }
    return $html
}

function Normalize-Genre {
    param([string]$g)
    if (-not $g) { return 'Other' }
    $t = $g.Trim().ToLowerInvariant()
    if ($t -match 'fantasy') { return 'Fantasy' }
    if ($t -match 'sci[\s\-]*fi|science[\s\-]*fiction|sf') { return 'Science-Fiction' }
    'Other'
}

# Extract number of pages (robust)
function Get-PageCountFromHtml {
    param([Parameter(Mandatory)][string]$Html)
    if (-not $Html -or $Html.Length -lt 1000) { return $null }
    foreach ($j in (Get-JsonLdStrings -Html $Html)) {
        if ($j -match '"@type"\s*:\s*"Book"') {
            $n = [regex]::Match($j, '"numberOfPages"\s*:\s*"?(?<p>\d{1,5})"?', 'IgnoreCase'); if ($n.Success) { return [int]$n.Groups['p'].Value }
        }
        $pc = [regex]::Match($j, '"pageCount"\s*:\s*(?<p>\d{1,5})', 'IgnoreCase'); if ($pc.Success) { return [int]$pc.Groups['p'].Value }
    }
    $m = [regex]::Match($Html, '<meta[^>]+itemprop="numberOfPages"[^>]+content="(?<p>\d{1,5})"', 'IgnoreCase'); if ($m.Success) { return [int]$m.Groups['p'].Value }
    $m = [regex]::Match($Html, '<span[^>]*itemprop="numberOfPages"[^>]*>\s*(?<p>\d{1,5})\s*pages?\s*</span>', 'IgnoreCase'); if ($m.Success) { return [int]$m.Groups['p'].Value }
    $m = [regex]::Match($Html, '<p[^>]*data-testid\s*=\s*"(?:pagesFormat|pages)"[^>]*>\s*(?<inner>[\s\S]*?)</p>', 'IgnoreCase')
    if ($m.Success) { $txt = Clean-Text $m.Groups['inner'].Value; $mp = [regex]::Match($txt, '(?i)\b(?<p>\d{1,5}(?:,\d{3})?)\s*pages?\b'); if ($mp.Success) { return [int](($mp.Groups['p'].Value) -replace ',', '') } }
    $anyScript = [regex]::Matches($Html, '<script[^>]*>(?<s>[\s\S]*?)</script>', 'IgnoreCase')
    foreach ($s in $anyScript) {
        $blob = $s.Groups['s'].Value
        $pc = [regex]::Match($blob, '"pageCount"\s*:\s*(?<p>\d{1,5})', 'IgnoreCase'); if ($pc.Success) { return [int]$pc.Groups['p'].Value }
        $np = [regex]::Match($blob, '"numberOfPages"\s*:\s*"?(?<p>\d{1,5})"?', 'IgnoreCase'); if ($np.Success) { return [int]$np.Groups['p'].Value }
        $np2= [regex]::Match($blob, '"numPages"\s*:\s*(?<p>\d{1,5})', 'IgnoreCase'); if ($np2.Success){ return [int]$np2.Groups['p'].Value }
    }
    $region = $Html; $anchor = [regex]::Match($Html, '(?i)data-testid="bookDetails"|data-testid="pagesFormat"|class="FeaturedDetails"|>\s*Book\s*Details\s*<')
    if ($anchor.Success) { $start = [Math]::Max(0, $anchor.Index - 5000); $len = [Math]::Min(90000, $Html.Length - $start); $region = $Html.Substring($start, $len) } else { $region = $Html.Substring(0, [Math]::Min(150000, $Html.Length)) }
    $clean = Clean-Text $region; $m = [regex]::Match($clean, '(?i)\b(?<p>\d{1,5}(?:,\d{3})?)\s*pages?\b')
    if ($m.Success) { return [int](($m.Groups['p'].Value) -replace ',', '') }
    return $null
}

# Extract robust rating + ratings count from a book page (many fallbacks)
function Get-BookRatingsFromHtml {
    param([Parameter(Mandatory)][string]$Html)
    $avg = $null; $count = $null

    # 1) JSON-LD (Book ➜ aggregateRating or AggregateRating block)
    foreach ($j in (Get-JsonLdStrings -Html $Html)) {
        if ($j -match '"@type"\s*:\s*"Book"') {
            $ar = [regex]::Match($j, '"aggregateRating"\s*:\s*\{(?<obj>[\s\S]+?)\}', 'IgnoreCase')
            if ($ar.Success) {
                $obj = $ar.Groups['obj'].Value
                $mv = [regex]::Match($obj, '"(?:ratingValue|averageRating)"\s*:\s*"?(?<v>\d(?:\.\d{1,2})?)"?', 'IgnoreCase')
                if ($mv.Success) { $avg = [double]$mv.Groups['v'].Value }
                $mc = [regex]::Match($obj, '"(?:ratingCount|reviewCount)"\s*:\s*"?(?<c>[\d,]+)"?', 'IgnoreCase')
                if ($mc.Success) { $count = Parse-Int $mc.Groups['c'].Value }
                if ($avg -or $count) { return @{ AvgRating=$avg; ReviewCount=$count } }
            }
        }
        if ($j -match '"@type"\s*:\s*"AggregateRating"') {
            $mv = [regex]::Match($j, '"(?:ratingValue|averageRating)"\s*:\s*"?(?<v>\d(?:\.\d{1,2})?)"?', 'IgnoreCase')
            if ($mv.Success) { $avg = [double]$mv.Groups['v'].Value }
            $mc = [regex]::Match($j, '"(?:ratingCount|reviewCount)"\s*:\s*"?(?<c>[\d,]+)"?', 'IgnoreCase')
            if ($mc.Success) { $count = Parse-Int $mc.Groups['c'].Value }
            if ($avg -or $count) { return @{ AvgRating=$avg; ReviewCount=$count } }
        }
    }

    # 2) Microdata/meta itemprops
    $mV = [regex]::Match($Html, '<meta[^>]+itemprop="ratingValue"[^>]+content="(?<v>[\d.]+)"', 'IgnoreCase')
    if ($mV.Success) { $avg = [double]$mV.Groups['v'].Value }
    $mC = [regex]::Match($Html, '<meta[^>]+itemprop="ratingCount"[^>]+content="(?<c>[\d,]+)"', 'IgnoreCase')
    if ($mC.Success) { $count = Parse-Int $mC.Groups['c'].Value }
    if ($avg -or $count) { return @{ AvgRating=$avg; ReviewCount=$count } }

    $sV = [regex]::Match($Html, '<span[^>]+itemprop="ratingValue"[^>]*>\s*(?<v>[\d.]+)\s*</span>', 'IgnoreCase')
    if ($sV.Success) { $avg = [double]$sV.Groups['v'].Value }
    $sC = [regex]::Match($Html, '<span[^>]+itemprop="ratingCount"[^>]*>\s*(?<c>[\d,]+)\s*</span>', 'IgnoreCase')
    if ($sC.Success) { $count = Parse-Int $sC.Groups['c'].Value }
    if ($avg -or $count) { return @{ AvgRating=$avg; ReviewCount=$count } }

    # 3) data-testid variations (new UI)
    $dtC = [regex]::Match($Html, '<[^>]+data-testid="(?:ratingsCount|ratingCount)"[^>]*>(?<t>[^<]+)</', 'IgnoreCase')
    if ($dtC.Success) { $count = Parse-Int $dtC.Groups['t'].Value }
    $dtV = [regex]::Match($Html, '<[^>]+data-testid="(?:rating|ratingValue)"[^>]*>\s*(?<v>\d(?:\.\d{1,2})?)\s*<', 'IgnoreCase')
    if ($dtV.Success) { $avg = [double]$dtV.Groups['v'].Value }
    if ($avg -or $count) { return @{ AvgRating=$avg; ReviewCount=$count } }

    # 4) Script blobs with aggregateRating or initial state
    $scripts = [regex]::Matches($Html, '<script[^>]*>(?<s>[\s\S]*?)</script>', 'IgnoreCase')
    foreach ($s in $scripts) {
        $blob = $s.Groups['s'].Value
        if ($blob -match '"aggregateRating"' -or $blob -match '"ratingsCount"') {
            $mv = [regex]::Match($blob, '"(?:ratingValue|averageRating)"\s*:\s*"?(?<v>\d(?:\.\d{1,2})?)"?', 'IgnoreCase')
            if ($mv.Success) { $avg = [double]$mv.Groups['v'].Value }
            $mc = [regex]::Match($blob, '"(?:ratingCount|ratingsCount|reviewCount)"\s*:\s*"?(?<c>[\d,]+)"?', 'IgnoreCase')
            if ($mc.Success) { $count = Parse-Int $mc.Groups['c'].Value }
            if ($avg -or $count) { return @{ AvgRating=$avg; ReviewCount=$count } }
        }
    }

    # 5) Cleaned text fallbacks
    $t = Clean-Text $Html
    $m = [regex]::Match($t, '(?<v>\d\.\d{1,2})\s*avg\s*rating', 'IgnoreCase')
    if ($m.Success) { $avg = [double]$m.Groups['v'].Value }
    $m2 = [regex]::Match($t, '\((?<c>[\d,]+)\s+ratings\)', 'IgnoreCase')
    if ($m2.Success) { $count = Parse-Int $m2.Groups['c'].Value }
    if ($avg -or $count) { return @{ AvgRating=$avg; ReviewCount=$count } }

    return @{ AvgRating=$null; ReviewCount=$null }
}

# Detect age category tags: Young Adult / Middle Grade / Children (may return multiple, '; '-joined)
function Get-AgeCategory {
    param(
        [string[]]$Genres,
        [string]$GenresHtml,
        [string]$FullHtml
    )
    if (-not $Genres) { $Genres = @() }
    if (-not $GenresHtml) { $GenresHtml = '' }
    if (-not $FullHtml) { $FullHtml = '' }

    $cats = New-Object System.Collections.Generic.HashSet[string] ([System.StringComparer]::OrdinalIgnoreCase)

    # Word-based (safe in genre labels)
    foreach ($g in $Genres) {
        $gl = $g.ToLowerInvariant()
        if ($gl -match '\byoung[\s-]*adult\b' -or $gl -match '\bya\b') { [void]$cats.Add('Young Adult') }
        if ($gl -match '\bmiddle[\s-]*grade\b' -or $gl -match '\bmg\b') { [void]$cats.Add('Middle Grade') }
        if ($gl -match '\bchildren(?:\x27|\u2019)?s\b' -or $gl -match '\bchildrens\b' -or $gl -match '\bkids\b') { [void]$cats.Add('Children') }
    }

    # Link-based (safe in HTML haystacks)
    foreach ($hay in @($GenresHtml,$FullHtml)) {
        if ($hay -match '(?i)/(genres|shelf/show)/(young-adult|ya)\b') { [void]$cats.Add('Young Adult') }
        if ($hay -match '(?i)/(genres|shelf/show)/middle-grade\b')     { [void]$cats.Add('Middle Grade') }
        if ($hay -match '(?i)/(genres|shelf/show)/(children|childrens|kids)\b') { [void]$cats.Add('Children') }
    }

    if ($cats.Count -eq 0) { return $null }
    # stable ordering
    $order = @('Children','Middle Grade','Young Adult')
    ($order | Where-Object { $cats.Contains($_) }) -join '; '
}

# Parse a single Goodreads book page to get core info + age category
function Get-BookPageDetails {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$BookUrl)
    $html = Get-CanonicalBookHtml -BookUrl $BookUrl

    # Title
    $title = $null
    foreach ($j in (Get-JsonLdStrings -Html $html)) {
        if ($j -match '"@type"\s*:\s*"Book"') {
            $mt = [regex]::Match($j, '"name"\s*:\s*"(?<nm>[^"]+)"', 'IgnoreCase')
            if ($mt.Success) { $title = [System.Net.WebUtility]::HtmlDecode($mt.Groups['nm'].Value); break }
        }
    }
    if (-not $title) {
        $mh1 = [regex]::Match($html, '<h1[^>]*data-testid="bookTitle"[^>]*>(?<t>[\s\S]*?)</h1>', 'IgnoreCase')
        if ($mh1.Success) { $title = [System.Net.WebUtility]::HtmlDecode((Strip-Tags $mh1.Groups['t'].Value)) }
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
    foreach ($j in (Get-JsonLdStrings -Html $html)) {
        $my = [regex]::Match($j, '"datePublished"\s*:\s*"(?<d>[^"]+)"', 'IgnoreCase')
        if ($my.Success) { $y = [regex]::Match($my.Groups['d'].Value, '\b(\d{4})\b'); if ($y.Success) { $pubYear = [int]$y.Groups[1].Value; break } }
    }
    if (-not $pubYear) {
        $m = [regex]::Match($html, '<meta[^>]+itemprop="datePublished"[^>]+content="(?<d>[^"]+)"', 'IgnoreCase')
        if ($m.Success) { $y = [regex]::Match($m.Groups['d'].Value, '\b(\d{4})\b'); if ($y.Success) { $pubYear = [int]$y.Groups[1].Value } }
    }
    if (-not $pubYear) {
        $m = [regex]::Match($html, '(?:First\s+)?Published[^0-9]{0,30}(\d{4})', 'IgnoreCase'); if ($m.Success) { $pubYear = [int]$m.Groups[1].Value }
    }

    # Genres (for age-category detection and info)
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
        foreach ($m in $ms) { $g = ([System.Net.WebUtility]::HtmlDecode($m.Groups['g'].Value)).Trim(); if ($g -and -not $genres.Contains($g)) { [void]$genres.Add($g) } }
    }

    $snippet = ''
    $blk = [regex]::Match($html, '(<section[^>]*genres[^>]*>[\s\S]{0,8000}?</section>)|(<div[^>]*genres[^>]*>[\s\S]{0,8000}?</div>)', 'IgnoreCase')
    if ($blk.Success) { $snippet = $blk.Value } else {
        $anchors = [regex]::Matches($html, '<a[^>]+href="/genres/[^"]+"[^>]*>[^<]+</a>', 'IgnoreCase')
        if ($anchors.Count -gt 0) { $sb = New-Object System.Text.StringBuilder; foreach ($a in $anchors) { [void]$sb.Append($a.Value) }; $snippet = $sb.ToString() }
    }

    $pages    = Get-PageCountFromHtml -Html $html
    $ratings  = Get-BookRatingsFromHtml -Html $html
    $hayLen   = [Math]::Min($html.Length, 150000)
    $haystack = ($snippet + ' ' + $html.Substring(0, $hayLen))
    $ageCat   = Get-AgeCategory -Genres $genres -GenresHtml $snippet -FullHtml $haystack

    [pscustomobject]@{
        Title          = $title
        PubYear        = $pubYear
        Genres         = $genres
        GenresHtml     = $snippet
        GenresHaystack = $haystack
        Pages          = $pages
        AvgRating      = $ratings.AvgRating
        ReviewCount    = $ratings.ReviewCount
        AgeCategory    = $ageCat
    }
}

function Get-BooksForAuthor {
    param(
        [Parameter(Mandatory)][string]$BaseTemplate,
        [Parameter(Mandatory)][string]$AuthorName,
        [string]$AuthorGenreNormalized = 'Other',
        [switch]$ShowProgress
    )

    # ── scrape list pages (NO numeric prefilter; collect everything) ─────
    $page=1; $rawRows=New-Object System.Collections.Generic.List[object]
    $firstHtml=(Get-Html ($BaseTemplate -f $page)).Content
    $totalPages= if ($firstHtml -match 'page\s+\d+\s+of\s+(\d+)'){[int]$matches[1]}else{$null}

    if ($ShowProgress){ Write-Progress -Id 11 -Activity "Scraping list pages ($AuthorName)" -Status "Start" -PercentComplete 0 }
    function Update-BarLocal { param($cur,$tot,$name,$show) if($show){ $pct=if($tot){[int](($cur-1)/$tot*100)}else{0}; Write-Progress -Id 11 -Activity "Scraping list pages ($name)" -Status "Page $cur$('/'+$tot)" -PercentComplete $pct } }

    $stopPaging = $false

    do {
        Update-BarLocal $page $totalPages $AuthorName $ShowProgress
        $html = if ($page -eq 1){$firstHtml}else{ (Get-Html ($BaseTemplate -f $page)).Content }

        # Track the minimum parsed ratings count seen on THIS page.
        $minCountOnPage = [int]::MaxValue
        $parsedAnyCount = $false

        foreach ($row in ($html -split '(?=<tr)')) {
            if ($row -notmatch 'class="bookTitle"') { continue }

            # Title + URL
            $titleFromRow=''; $bookUrl=$null
            $mTitle = [regex]::Match($row,'<a[^>]*class="bookTitle"[^>]*href="(?<href>[^"]+)"[^>]*>(?<inner>[\s\S]*?)</a>','IgnoreCase,Singleline')
            if ($mTitle.Success) {
                $href = $mTitle.Groups['href'].Value
                $bookUrl = if ($href -like 'http*') { $href } else { "https://www.goodreads.com$href" }
                $inner = $mTitle.Groups['inner'].Value
                $mName = [regex]::Match($inner,'<span[^>]*itemprop="name"[^>]*>(?<t>[^<]+)</span>','IgnoreCase')
                if ($mName.Success) { $titleFromRow = [System.Net.WebUtility]::HtmlDecode($mName.Groups['t'].Value).Trim() }
                else { $titleFromRow = [System.Net.WebUtility]::HtmlDecode((Strip-Tags $inner)) }
            } else { continue }

            # Parse any row-level hints (we do not filter here)
            $pubYear=$null; $mYear=[regex]::Match($row,'published\s+(?:\w+\s+)?(\d{4})','IgnoreCase'); if ($mYear.Success){ $pubYear=[int]$mYear.Groups[1].Value }
            $seriesName,$seriesNum=$null,$null
            $m=[regex]::Match($row,'\(([^#(]+)#\s*([\d]+(?:\.\d+)?)')
            if ($m.Success) {
                $seriesName = ($m.Groups[1].Value -replace '\s+$','').Trim()
                $numString  = $m.Groups[2].Value
                $tmp=0.0; [double]::TryParse($numString,[System.Globalization.NumberStyles]::Float,[System.Globalization.CultureInfo]::InvariantCulture,[ref]$tmp) | Out-Null
                if (-not [double]::IsNaN($tmp)) { $seriesNum = $tmp }
            }

            # Parse ratings COUNT on the list row for early-stop heuristic
            $mCount=[regex]::Match($row,'([\d,]+)\s*(?:ratings|reviews)','IgnoreCase')
            if ($mCount.Success) {
                $parsedAnyCount = $true
                $c = Parse-Int $mCount.Groups[1].Value
                if ($c -lt $minCountOnPage) { $minCountOnPage = $c }
            }

            $rawRows.Add([pscustomobject]@{
                TitleRow    = $titleFromRow
                Url         = $bookUrl
                PubYear     = $pubYear
                SeriesName  = $seriesName
                SeriesNum   = $seriesNum
            })
        }

        # decide if we should stop paging AFTER this page
        $hasNext = ($html -match 'rel="next"')
        if ($parsedAnyCount -and $minCountOnPage -lt 1000) {
            $stopPaging = $true
        }
        if ($stopPaging) { $hasNext = $false }

        $page++
        Start-Sleep -Milliseconds (Get-Random -Min 800 -Max 1600)
    } while ($hasNext)

    if ($ShowProgress){ Write-Progress -Id 11 -Activity "Scraping list pages ($AuthorName)" -Completed }

    if (-not $rawRows -or $rawRows.Count -eq 0) {
        Write-Warning "No titles found on the author list for '$AuthorName'."
        return @()
    }

    # ── verify: fetch book pages; compute details; APPLY thresholds here ─
    $verified = New-Object System.Collections.Generic.List[object]
    $idx=0
    foreach ($b in $rawRows) {
        $idx++; if ($ShowProgress) { $pct=[int](($idx/$rawRows.Count)*100); Write-Progress -Id 12 -Activity "Verifying book pages ($AuthorName)" -Status $b.TitleRow -PercentComplete $pct }
        try {
            $details = Get-BookPageDetails -BookUrl $b.Url

            $finalTitle = if ($details.Title -and $details.Title -notmatch '^(?i)goodreads\b') { $details.Title } else { $b.TitleRow }
            $year = if ($details.PubYear) { $details.PubYear } else { $b.PubYear }
            if (-not $year) { $year = [int]::MaxValue } # keep unknown year, sort last

            # Determine Non-fiction via genres haystack
            $isNF = $false
            if ($details.Genres -contains 'Nonfiction' -or $details.GenresHaystack -match '(?i)\bnon[- ]?fiction\b') { $isNF = $true }

            # Ratings from the book page
            $avgRating   = $details.AvgRating
            $reviewCount = $details.ReviewCount

            # Apply thresholds: keep only strong titles
            $qualifies = $false
            if ($avgRating -ne $null -and $reviewCount -ne $null) {
                $qualifies = ($avgRating -ge 4.0 -and (
                    ($isNF   -and $reviewCount -ge 10000) -or
                    (-not $isNF -and $reviewCount -ge 1000)
                ))
            }

            if ($qualifies) {
                $verified.Add([pscustomobject]@{
                    Author       = $AuthorName
                    AuthorGenre  = $AuthorGenreNormalized
                    Title        = $finalTitle
                    Url          = $b.Url
                    Category     = if ($isNF) { 'Non-fiction' } else { 'Fiction' }
                    AvgRating    = [math]::Round([double]$avgRating,2)
                    ReviewCount  = [int]$reviewCount
                    PubYear      = $year
                    SeriesName   = $b.SeriesName
                    SeriesNum    = $b.SeriesNum
                    Pages        = $details.Pages
                    AgeCategory  = $details.AgeCategory
                })
            }
        } catch { }
        Start-Sleep -Milliseconds (Get-Random -Min 800 -Max 1600)
    }
    if ($ShowProgress) { Write-Progress -Id 12 -Activity "Verifying book pages ($AuthorName)" -Completed }
    if ($verified.Count -eq 0) { return @() }

    # ── compute earliest year per block (series or stand-alone) ─────────
    $firstYear=@{}
    foreach ($b in $verified) {
        $key = if ($b.SeriesName) { "$AuthorName|$($b.SeriesName)" } else { "$AuthorName|$($b.Title)" }
        if (-not $firstYear.ContainsKey($key) -or $firstYear[$key] -gt $b.PubYear) { $firstYear[$key]=$b.PubYear }
    }
    foreach ($b in $verified) {
        $key = if ($b.SeriesName) { "$AuthorName|$($b.SeriesName)" } else { "$AuthorName|$($b.Title)" }
        $b | Add-Member -NotePropertyName BlockStartYear -NotePropertyValue $firstYear[$key]
        if ($b.SeriesNum -eq $null) { $b | Add-Member -Force SeriesNum ([double]::PositiveInfinity) }
    }

    ,$verified
}

# ── gather inputs (now capturing Genre) ─────────────────────────────────
$inputSpecs = New-Object System.Collections.Generic.List[pscustomobject]

# From params
if ($Author) { foreach($a in $Author){ if ($a) { $inputSpecs.Add([pscustomobject]@{ Author=$a; Url=$null; Genre=$null }) } } }
if ($Url)    { foreach($u in $Url)   { if ($u) { $inputSpecs.Add([pscustomobject]@{ Author=$null; Url=$u; Genre=$null }) } } }

# From CSV
if ($InCsv) {
    try { $rows = Import-Csv -Path $InCsv } catch { Write-Error "Failed to read CSV '$InCsv': $($_.Exception.Message)"; return }
    foreach ($row in $rows) {
        # Flexible genre column detection
        $genreProp = ($row.PSObject.Properties.Name | Where-Object { $_ -match '^(?i)(genre|genres|category|categories)$' } | Select-Object -First 1)
        $genreVal  = if ($genreProp) { $row.$genreProp } else { $null }

        # Author-like fields
        $authorFields = @('Author','Authors','Name')
        foreach ($f in $authorFields) {
            if ($row.PSObject.Properties.Name -contains $f -and $row.$f) {
                foreach ($a in ($row.$f -split '[,;]' | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
                    $inputSpecs.Add([pscustomobject]@{ Author=$a; Url=$null; Genre=$genreVal })
                }
            }
        }

        # URL-like fields
        $urlFields = @('Url','URL','ListUrl','AuthorUrl','AuthorURL')
        foreach ($f in $urlFields) {
            if ($row.PSObject.Properties.Name -contains $f -and $row.$f) {
                foreach ($u in ($row.$f -split '[,;]' | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
                    $inputSpecs.Add([pscustomobject]@{ Author=$null; Url=$u; Genre=$genreVal })
                }
            }
        }
    }
}

if ($inputSpecs.Count -eq 0) { Write-Warning "No authors or URLs found after parsing inputs."; return }

# ── build worklist (resolve templates, display names, attach Genre) ─────
$work = New-Object System.Collections.Generic.List[pscustomobject]
$seenTemplates = @{}

foreach ($spec in $inputSpecs) {
    try {
        $tmpl = if ($spec.Author) { Resolve-GoodreadsAuthorListBaseUrl -Author $spec.Author } else { Resolve-GoodreadsAuthorListBaseUrl -Url $spec.Url }
        if (-not $seenTemplates.ContainsKey($tmpl)) {
            $aid  = Get-AuthorIdFromListTemplate $tmpl
            $name = if ($aid) { Get-AuthorDisplayNameById -AuthorId $aid } else { if ($spec.Author) { $spec.Author } else { "Author from URL" } }
            $genreNorm = Normalize-Genre $spec.Genre
            $work.Add([pscustomobject]@{ Template=$tmpl; AuthorId=$aid; AuthorName=$name; Genre=$genreNorm })
            $seenTemplates[$tmpl] = $true
        }
    } catch {
        $who = if ($spec.Author){$spec.Author}else{$spec.Url}
        Write-Warning "Skipping '$who': $($_.Exception.Message)"
    }
}

if ($work.Count -eq 0) { Write-Warning "Nothing to process after resolving authors/urls."; return }

# ── scrape all requested authors ────────────────────────────────────────
$all = New-Object System.Collections.Generic.List[object]
foreach ($w in $work) {
    $items = Get-BooksForAuthor -BaseTemplate $w.Template -AuthorName $w.AuthorName -AuthorGenreNormalized $w.Genre -ShowProgress:$ShowProgress
    foreach ($it in $items) { [void]$all.Add($it) }
}
if ($all.Count -eq 0) { Write-Warning "No books met filters across all authors."; return }

# ── compute per-author averages and attach Genre ────────────────────────
$authorSummary = New-Object System.Collections.Generic.List[object]
$groups = $all | Group-Object Author
$authorAvgMap = @{}; $authorGenreMap=@{}

foreach ($g in $groups) {
    $avg = $null
    $avgRaw = (($g.Group | Where-Object { $_.AvgRating -ne $null } | Measure-Object -Property AvgRating -Average).Average)
    if ($avgRaw -is [double]) { $avg = [math]::Round($avgRaw, 2) }
    $ag = ($g.Group | Select-Object -ExpandProperty AuthorGenre -First 1); if (-not $ag) { $ag = 'Other' }
    $authorAvgMap[$g.Name] = $avg; $authorGenreMap[$g.Name]=$ag
    $authorSummary.Add([pscustomobject]@{ Author=$g.Name; Genre=$ag; Books=$g.Count; AuthorAvg=$avg }) | Out-Null
}

foreach ($row in $all) {
    $row | Add-Member -NotePropertyName AuthorAvg -NotePropertyValue $authorAvgMap[$row.Author] -Force
    if (-not $row.PSObject.Properties.Match('AuthorGenre')) { $row | Add-Member -NotePropertyName AuthorGenre -NotePropertyValue ($authorGenreMap[$row.Author]) -Force }
}

# ── build interleaved author order with arrays (no Queues) ──────────────
function Sort-ByAvgDesc {
    param($seq)
    $seq | Sort-Object `
        @{ Expression = { if ($_.AuthorAvg -eq $null) { [double]::NegativeInfinity } else { [double]$_.AuthorAvg } }; Descending = $true }, `
        @{ Expression = 'Author' ; Descending = $false }
}

$fantasyList = Sort-ByAvgDesc ($authorSummary | Where-Object { $_.Genre -eq 'Fantasy' })
$scifiList   = Sort-ByAvgDesc ($authorSummary | Where-Object { $_.Genre -eq 'Science-Fiction' })
$otherList   = Sort-ByAvgDesc ($authorSummary | Where-Object { $_.Genre -eq 'Other' })

# Indices into each list
$script:fi  = 0  # Fantasy index
$script:sfi = 0  # Science-Fiction index
$script:oi  = 0  # Other index

function Take-FromGenre {
    param([ValidateSet('Fantasy','Science-Fiction','Other')] [string]$Genre)
    switch ($Genre) {
        'Fantasy' {
            if ($script:fi -lt $fantasyList.Count) {
                $item = $fantasyList[$script:fi]; $script:fi++
                return $item
            }
        }
        'Science-Fiction' {
            if ($script:sfi -lt $scifiList.Count) {
                $item = $scifiList[$script:sfi]; $script:sfi++
                return $item
            }
        }
        'Other' {
            if ($script:oi -lt $otherList.Count) {
                $item = $otherList[$script:oi]; $script:oi++
                return $item
            }
        }
    }
    return $null
}

function Take-Any {
    $x = Take-FromGenre 'Fantasy'         ; if ($x) { return $x }
    $x = Take-FromGenre 'Science-Fiction' ; if ($x) { return $x }
    $x = Take-FromGenre 'Other'           ; if ($x) { return $x }
    return $null
}

$pattern = @('Fantasy','Science-Fiction','Fantasy','Other')  # rows: 1=F,2=SF,3=F,4=Other, repeat
$authorInterleaved = New-Object System.Collections.Generic.List[object]
$idx = 0

while ($script:fi -lt $fantasyList.Count -or $script:sfi -lt $scifiList.Count -or $script:oi -lt $otherList.Count) {
    $want = $pattern[$idx % $pattern.Count]
    $picked = Take-FromGenre $want
    if (-not $picked) { $picked = Take-Any }
    if ($picked) { $authorInterleaved.Add($picked) }
    $idx++
}

# Add 1-based row numbers for display
for ($i=0; $i -lt $authorInterleaved.Count; $i++) { $authorInterleaved[$i] | Add-Member -NotePropertyName Row -NotePropertyValue ($i+1) -Force }

# Map author to interleaved index
$authorOrder = @{}; for ($i=0; $i -lt $authorInterleaved.Count; $i++) { $authorOrder[$authorInterleaved[$i].Author] = $i }

# ── final sort for detailed book rows: interleaved authors; blocks by earliest year; series grouped; within series by number; then year/title ─
$sorted = $all |
Sort-Object `
    @{Expression = { $authorOrder[$_.Author] } ; Ascending = $true}, `
    @{Expression = 'BlockStartYear'            ; Ascending = $true}, `
    @{Expression = { if ($_.SeriesName) { $_.SeriesName } else { $_.Title } } ; Ascending = $true}, `
    @{Expression = 'SeriesNum'                 ; Ascending = $true}, `
    @{Expression = 'PubYear'                   ; Ascending = $true}, `
    @{Expression = 'Title'                     ; Ascending = $true}

# ── AUTHOR RANKING (interleaved) ────────────────────────────────────────
"`nAuthor ranking (pattern: Row 1 F, 2 SF, 3 F, 4 Other; then repeat):`n" | Write-Host
$authorInterleaved |
Select-Object `
    @{l='Row'       ; e={ $_.Row }}, `
    @{l='Author'    ; e={ $_.Author }}, `
    @{l='Genre'     ; e={ $_.Genre }}, `
    @{l='Books'     ; e={ $_.Books }}, `
    @{l='AuthorAvg' ; e={ if ($_.AuthorAvg -ne $null) { '{0:N2}' -f [double]$_.AuthorAvg } else { $null } }} |
Format-Table -AutoSize

# ── DETAILED BOOK ROWS (console) ────────────────────────────────────────
$tableRows = $sorted |
Select-Object `
    @{Label='Author'     ; Expression = { $_.Author }}, `
    @{Label='Genre'      ; Expression = { $_.AuthorGenre }}, `
    @{Label='AuthorAvg'  ; Expression = { if ($_.AuthorAvg -ne $null) { '{0:N2}' -f [double]$_.AuthorAvg } else { $null } }}, `
    @{Label='Title'      ; Expression = { $_.Title }}, `
    @{Label='SeriesName' ; Expression = { $_.SeriesName }}, `
    @{Label='SeriesNum'  ; Expression = { if ([double]::IsInfinity($_.SeriesNum)) { $null } else { $_.SeriesNum } }}, `
    @{Label='PubYear'    ; Expression = { if ($_.PubYear -eq [int]::MaxValue) { $null } else { $_.PubYear } }}, `
    @{Label='Pages'      ; Expression = { $_.Pages }}, `
    @{Label='AgeCategory'; Expression = { $_.AgeCategory }}, `
    @{Label='AvgRating'  ; Expression = { if ($_.AvgRating -ne $null) { '{0:N2}' -f [double]$_.AvgRating } else { $null } }}, `
    @{Label='ReviewCount'; Expression = { if ($_.ReviewCount -ne $null) { '{0:N0}' -f [int]$_.ReviewCount } else { $null } }}, `
    @{Label='Url'        ; Expression = { $_.Url }}

$tableRows | Format-Table -AutoSize -Wrap

# ── CSV rows (single file only) ─────────────────────────────────────────
$csvRows = $sorted |
Select-Object `
    @{Name='Rank'       ; Expression = { $authorOrder[$_.Author] + 1 }}, `
    @{Name='Author'     ; Expression = { $_.Author }}, `
    @{Name='Genre'      ; Expression = { $_.AuthorGenre }}, `
    @{Name='AuthorAvg'  ; Expression = { if ($_.AuthorAvg -ne $null) { [math]::Round([double]$_.AuthorAvg,2) } else { $null } }}, `
    @{Name='Title'      ; Expression = { $_.Title }}, `
    @{Name='SeriesName' ; Expression = { $_.SeriesName }}, `
    @{Name='SeriesNum'  ; Expression = { if ([double]::IsInfinity($_.SeriesNum) -or $null -eq $_.SeriesNum) { $null } else { $_.SeriesNum } }}, `
    @{Name='PubYear'    ; Expression = { if ($_.PubYear -eq [int]::MaxValue) { $null } else { $_.PubYear } }}, `
    @{Name='Pages'      ; Expression = { if ($_.Pages) { [int]$_.Pages } else { $null } }}, `
    @{Name='AgeCategory'; Expression = { $_.AgeCategory }}, `
    @{Name='AvgRating'  ; Expression = { if ($_.AvgRating -ne $null) { [math]::Round([double]$_.AvgRating,2) } else { $null } }}, `
    @{Name='ReviewCount'; Expression = { if ($_.ReviewCount -ne $null) { [int]$_.ReviewCount } else { $null } }}, `
    @{Name='Url'        ; Expression = { $_.Url }}

if ($OutCsv) {
    try {
        $dir = Split-Path -Parent $OutCsv
        if ($dir -and -not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
        $csvRows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutCsv
        Write-Host "Saved CSV to: $OutCsv"
    } catch {
        Write-Warning "Failed to write CSV: $($_.Exception.Message)"
    }
}
