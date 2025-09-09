<#
.SYNOPSIS
  Combined scraper for “List of <Service> original programming” Wikipedia pages.

  - Works with multiple URLs (Netflix + HBO + HBO Max + Apple TV+ + others)
  - Skips rows in "Upcoming" section(s)
  - Default filter: Premiere is in the target Year AND includes a month/day (not year-only)
  - Optional filter A (relative window): -OnlyTwoMonthsAgoMonth limits to the entire month from two months ago
    (e.g., if current month is September, only July 1 – July 31)
  - Optional filter B (explicit window): -FilterMonth <name|number> AND -FilterYear <yyyy> (both required together)
    When used, the Year regex filter is ignored and only titles within that exact month are included.
  - IMDb: by default keeps only shows with Rating >= MinRating and Votes >= MinVotes
          OR includes rows with lookup failures ("IMDb not found" / "IMDb lookup error")
  - Adds Network column (auto from URL)
  - DEFAULT WRITE BEHAVIOR: merges existing CSV + new rows, de-dups by identity key,
    then sorts the ENTIRE file by IMDb rating DESC on write (numeric first; not-found/errors last).

.OUTPUT
  Objects with Title, Genre, Premiere, Network, ImdbRating
#>

[CmdletBinding(DefaultParameterSetName='Default')]
param(
  [string[]]$Urls = @(
    'https://en.wikipedia.org/wiki/List_of_Netflix_original_programming',
    'https://en.wikipedia.org/wiki/List_of_HBO_original_programming#Upcoming_programming',
    'https://en.wikipedia.org/wiki/List_of_HBO_Max_original_programming',
    'https://en.wikipedia.org/wiki/List_of_Apple_TV%2B_original_programming',
    'https://en.wikipedia.org/wiki/List_of_Paramount%2B_original_programming',
    'https://en.wikipedia.org/wiki/List_of_Disney%2B_original_programming',
    'https://en.wikipedia.org/wiki/List_of_Hulu_original_programming',
    'https://en.wikipedia.org/wiki/List_of_Peacock_original_programming',
    'https://en.wikipedia.org/wiki/List_of_Amazon_Prime_Video_original_programming',
    'https://en.wikipedia.org/wiki/List_of_MGM%2B_original_programming',
    'https://en.wikipedia.org/wiki/List_of_Discovery%2B_original_programming',
    'https://en.wikipedia.org/wiki/List_of_Starz_original_programming',
    'https://en.wikipedia.org/wiki/List_of_programs_broadcast_by_FX',
    'https://en.wikipedia.org/wiki/List_of_programs_broadcast_by_AMC'
  ),
  [string]$OutputCsv = "C:\Personal Scripts\CurrentMonthTVShows.csv",
  [int]$Year = 2025,
  [double]$MinRating = 8.4,
  [int]$MinVotes = 1000,
  [int]$RequestDelayMs = 250,
  [switch]$IncludeBelowThreshold,

  # Relative month window
  [Parameter(ParameterSetName='RelativeMonth')]
  [switch]$OnlyTwoMonthsAgoMonth,

  # Option B: explicit month & year (paired and both required)
  [Parameter(ParameterSetName='ExplicitMonth', Mandatory=$true)]
  [string]$FilterMonth,
  [Parameter(ParameterSetName='ExplicitMonth', Mandatory=$true)]
  [int]$FilterYear
)

function Remove-Html {
  param([string]$Html)
  if ([string]::IsNullOrWhiteSpace($Html)) { return $null }
  $s = $Html
  $s = $s -replace '<sup[^>]*>.*?</sup>', ''                 # citation superscripts
  $s = $s -replace '<span[^>]*class="nowrap"[^>]*>', ''       # unwrap nowrap spans
  $s = $s -replace '<br\s*/?>', '; '                          # <br> => separator
  $s = $s -replace '<[^>]+>', ''                              # strip tags
  $s = [System.Net.WebUtility]::HtmlDecode($s)                # decode entities
  $s = $s -replace '\[\d+\]', ''                              # [1]
  $s = $s -replace '\s{2,}', ' '                              # collapse whitespace
  $s.Trim()
}

function Remove-Diacritics {
  param([Parameter(Mandatory)][string]$Text)
  $norm = $Text.Normalize([Text.NormalizationForm]::FormD)
  $sb = New-Object System.Text.StringBuilder
  foreach ($ch in $norm.ToCharArray()) {
    if (-not [Globalization.CharUnicodeInfo]::GetUnicodeCategory($ch) -eq [Globalization.UnicodeCategory]::NonSpacingMark) {
      [void]$sb.Append($ch)
    }
  }
  $sb.ToString().Normalize([Text.NormalizationForm]::FormC)
}

function Clean-TitleForSearch {
  param([string]$Title)
  if (-not $Title) { return $Title }
  $t = [System.Net.WebUtility]::HtmlDecode($Title)
  $t = Remove-Diacritics $t
  $t = $t -replace '\s*\([^)]*\)\s*', ''     # drop parentheticals
  $t = $t -replace '[:–—\-&]+', ' '          # normalize punctuation and &
  $t = $t -replace '\s{2,}', ' '
  $t.Trim()
}

function Get-FirstYearFromText {
  param([string]$Text)
  if ([string]::IsNullOrWhiteSpace($Text)) { return $null }
  $m = [regex]::Match($Text, '\b(19|20)\d{2}\b')
  if ($m.Success) { [int]$m.Value } else { $null }
}

function Invoke-Http {
  param([Parameter(Mandatory)] [string]$Uri)
  Invoke-WebRequest -Uri $Uri -Headers @{
    'User-Agent'      = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) PowerShell scraper'
    'Accept-Language' = 'en-US,en;q=0.9'
  } -ErrorAction Stop
}

# Determine Network/Service from URL title
function Get-NetworkFromUrlTitle {
  param([Parameter(Mandatory)][string]$Url)
  try {
    $u = [uri]$Url
    $title = $u.Segments[$u.Segments.Count-1]  # trailing segment
    $decoded = [System.Net.WebUtility]::UrlDecode($title) -replace '_',' '
    $m = [regex]::Match($decoded, '^(?i)List of (.+?) original programming')
    if ($m.Success) { return $m.Groups[1].Value.Trim() }
  } catch { }
  return $null
}

# Find where the “Upcoming” section begins for a page
function Get-CutoffIndex {
  param([Parameter(Mandatory)][string]$Html, [string]$Url)
  $ids = @(
    'Upcoming[_\s]original[_\s]programming',  # Netflix phrasing
    'Upcoming[_\s]programming'                # HBO/HBO Max/Apple TV+ phrasing
  )
  foreach ($id in $ids) {
    $re = [regex]::new('id\s*=\s*["'']' + $id + '["'']', 'IgnoreCase')
    $m = $re.Match($Html)
    if ($m.Success) { return $m.Index }
  }
  $txts = @(
    '>\s*Upcoming\s+original\s+programming\s*<',
    '>\s*Upcoming\s+programming\s*<'
  )
  foreach ($pat in $txts) {
    $re = [regex]::new($pat, 'IgnoreCase')
    $m = $re.Match($Html)
    if ($m.Success) { return $m.Index }
  }
  if ($Url -match 'Netflix') {
    $ab = [regex]::Matches($Html, '(?i)The\s+Abandons')
    if ($ab.Count -gt 0) { return $ab[$ab.Count-1].Index }
  }
  return -1
}

# Wikidata helpers
function Get-WikiTitleFromHref {
  param([string]$Href)
  if ([string]::IsNullOrWhiteSpace($Href)) { return $null }
  $m = [regex]::Match($Href, '^/wiki/([^?#]+)')
  if (-not $m.Success) { return $null }
  $slug = $m.Groups[1].Value
  $decoded = [System.Net.WebUtility]::UrlDecode($slug)
  ($decoded -replace '_',' ')
}

function Get-WikidataQidFromEnwikiTitle {
  param([Parameter(Mandatory)][string]$EnwikiTitle,[int]$DelayMs=250)
  try {
    $api = "https://en.wikipedia.org/w/api.php?action=query&format=json&prop=pageprops&ppprop=wikibase_item&titles=" +
           [System.Uri]::EscapeDataString($EnwikiTitle)
    Start-Sleep -Milliseconds $DelayMs
    $resp = Invoke-Http -Uri $api
    $j = $resp.Content | ConvertFrom-Json
    $page = ($j.query.pages.PSObject.Properties | Select-Object -First 1).Value
    if ($page -and $page.pageprops -and $page.pageprops.wikibase_item) { return $page.pageprops.wikibase_item }
  } catch {}
  return $null
}

function Get-ImdbIdFromWikidataQid {
  param([Parameter(Mandatory)][string]$Qid,[int]$DelayMs=250)
  try {
    $url = "https://www.wikidata.org/wiki/Special:EntityData/$Qid.json"
    Start-Sleep -Milliseconds $DelayMs
    $resp = Invoke-Http -Uri $url
    $j = $resp.Content | ConvertFrom-Json
    $entity = $j.entities.$Qid
    if ($entity -and $entity.claims -and $entity.claims.P345) {
      foreach ($cl in $entity.claims.P345) {
        $val = $cl.mainsnak.datavalue.value
        if ($val -match '^tt\d+$') { return $val }
      }
    }
  } catch {}
  return $null
}

# IMDb helpers
function Try-GetImdbIdFromWikipediaPage {
  param([Parameter(Mandatory)][string]$WikiHref,[int]$DelayMs=250)
  try {
    $uri = $WikiHref
    if ($uri -notmatch '^https?://') { $uri = 'https://en.wikipedia.org' + $WikiHref }
    Start-Sleep -Milliseconds $DelayMs
    $resp = Invoke-Http -Uri $uri
    $m = [regex]::Match($resp.Content, '/title/(tt\d+)', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($m.Success) { return $m.Groups[1].Value }
  } catch {}
  return $null
}

function Try-GetImdbIdFromWebSearch {
  param(
    [Parameter(Mandatory)][string]$Title,
    [int]$PremiereYear,
    [int]$DelayMs=250,
    [string]$NetworkHint
  )
  $queries = @()
  $clean = Clean-TitleForSearch $Title
  if ($PremiereYear) { $queries += "$clean ($PremiereYear) site:imdb.com/title" }

  if ($NetworkHint) {
    $hints = New-Object System.Collections.Generic.List[string]
    $hints.Add($NetworkHint)
    if ($NetworkHint -match '(?i)Apple\s*TV\+') { $hints.Add(($NetworkHint -replace '\+',' Plus')) }
    if ($NetworkHint -match '(?i)\bHBO Max\b') { $hints.Add('Max') }
    foreach ($h in $hints) {
      $queries += "$clean `"$h`" site:imdb.com/title"
    }
  }

  $queries += "$clean site:imdb.com/title"

  foreach ($q in $queries) {
    $enc = [System.Uri]::EscapeDataString($q)
    foreach ($engine in @('bing','ddg')) {
      try {
        $url = if ($engine -eq 'bing') { "https://www.bing.com/search?q=$enc" } else { "https://duckduckgo.com/html/?q=$enc" }
        Start-Sleep -Milliseconds $DelayMs
        $resp = Invoke-Http -Uri $url
        $m = [regex]::Match($resp.Content, '/title/(tt\d+)', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        if ($m.Success) { return $m.Groups[1].Value }
      } catch { continue }
    }
  }
  return $null
}

function Get-ImdbRating {
  param([Parameter(Mandatory)][string]$ImdbId)
  try {
    $titleUrl = "https://www.imdb.com/title/$ImdbId/"
    $resp = Invoke-Http -Uri $titleUrl
    $ldRe = [regex]::new('<script[^>]+type=["'']application/ld\+json["''][^>]*>(.*?)</script>', 'IgnoreCase,Singleline')
    $best = $null
    foreach ($m in $ldRe.Matches($resp.Content)) {
      $jsonText = $m.Groups[1].Value
      try {
        $j = $jsonText | ConvertFrom-Json
        $objs = @()
        if ($j -is [System.Collections.IEnumerable] -and -not ($j -is [string])) { $objs = $j } else { $objs = @($j) }
        foreach ($o in $objs) {
          if ($o.aggregateRating -and $o.aggregateRating.ratingValue -and $o.aggregateRating.ratingCount) { $best = $o; break }
        }
        if ($best) { break }
      } catch { continue }
    }
    if (-not $best) { return @{ Status='not_found' } }
    $val = [double]$best.aggregateRating.ratingValue
    $cntRaw = $best.aggregateRating.ratingCount
    if ($cntRaw -isnot [int]) { $cntRaw = ($cntRaw.ToString() -replace ',', '') }
    $cnt = [int]$cntRaw
    return @{ Status='ok'; Id=$ImdbId; Rating=$val; Votes=$cnt }
  } catch {
    return @{ Status='error' }
  }
}

function Get-ImdbAssessment {
  <#
    Returns:
      @{ Status='ok'; Id='tt…'; Rating=[double]; Votes=[int] }
      @{ Status='not_found' } / @{ Status='error' }
    Tries: IMDb suggestion → IMDb find → Wikidata P345 → Wikipedia ext. link → web search
  #>
  param(
    [Parameter(Mandatory)] [string]$Title,
    [int]$PremiereYear,
    [int]$DelayMs = 250,
    [string]$TitleHref,
    [string]$NetworkHint
  )

  try {
    foreach ($queryTitle in @($Title, (Clean-TitleForSearch $Title))) {
      if ([string]::IsNullOrWhiteSpace($queryTitle)) { continue }
      try {
        $firstLetter = ($queryTitle.Trim())[0].ToString().ToLower()
        $sugUrl = "https://v2.sg.media-imdb.com/suggestion/$firstLetter/" + [System.Uri]::EscapeDataString($queryTitle) + ".json"
        $sugResp = Invoke-Http -Uri $sugUrl
        $json = $sugResp.Content | ConvertFrom-Json
        if ($json -and $json.d) {
          $scored = foreach ($d in $json.d) {
            if (-not ($d.id -match '^tt\d+')) { continue }
            $score = 0
            if ($d.l -eq $queryTitle) { $score += 2 }
            if ($PremiereYear -and $d.y -eq $PremiereYear) { $score += 3 }
            elseif ($PremiereYear -and $d.yr -and ($d.yr -match [regex]::Escape("$PremiereYear"))) { $score += 2 }
            if ($d.q -match '(?i)TV') { $score += 1 }
            [pscustomobject]@{ Id=$d.id; Score=$score }
          }
          if ($scored) {
            $ttId = ($scored | Sort-Object Score -Descending | Select-Object -First 1).Id
            if ($ttId) { Start-Sleep -Milliseconds $DelayMs; return Get-ImdbRating -ImdbId $ttId }
          }
        }
      } catch { }
    }

    try {
      $findUrl = "https://www.imdb.com/find/?s=tt&q=" + [System.Uri]::EscapeDataString($Title)
      $findResp = Invoke-Http -Uri $findUrl
      $m = [regex]::Match($findResp.Content, '/title/(tt\d+)/')
      if ($m.Success) {
        $ttId = $m.Groups[1].Value
        Start-Sleep -Milliseconds $DelayMs
        return Get-ImdbRating -ImdbId $ttId
      }
    } catch { }

    $qid = $null
    if ($TitleHref) {
      $pageTitle = Get-WikiTitleFromHref $TitleHref
      if ($pageTitle) { $qid = Get-WikidataQidFromEnwikiTitle -EnwikiTitle $pageTitle -DelayMs $DelayMs }
    }
    if (-not $qid) { $qid = Get-WikidataQidFromEnwikiTitle -EnwikiTitle $Title -DelayMs $DelayMs }
    if ($qid) {
      $tt2 = Get-ImdbIdFromWikidataQid -Qid $qid -DelayMs $DelayMs
      if ($tt2) { Start-Sleep -Milliseconds $DelayMs; return Get-ImdbRating -ImdbId $tt2 }
    }

    if ($TitleHref) {
      $tt3 = Try-GetImdbIdFromWikipediaPage -WikiHref $TitleHref -DelayMs $DelayMs
      if ($tt3) { Start-Sleep -Milliseconds $DelayMs; return Get-ImdbRating -ImdbId $tt3 }
    }

    $tt4 = Try-GetImdbIdFromWebSearch -Title $Title -PremiereYear $PremiereYear -DelayMs $DelayMs -NetworkHint $NetworkHint
    if ($tt4) { Start-Sleep -Milliseconds $DelayMs; return Get-ImdbRating -ImdbId $tt4 }

    return @{ Status='not_found' }
  } catch {
    return @{ Status='error' }
  }
}

# Parse Premiere into a usable DateTime (first date found in the cell)
function Get-PremiereDate {
  param([string]$Premiere)
  if ([string]::IsNullOrWhiteSpace($Premiere)) { return $null }
  $s = ($Premiere -split ';')[0].Trim()
  $s = ($s -split '[–—]')[0].Trim()
  $culture = [System.Globalization.CultureInfo]::GetCultureInfo('en-US')
  $styles  = [System.Globalization.DateTimeStyles]::AssumeLocal

  $m = [regex]::Match($s, '\b\d{4}-\d{2}-\d{2}\b')
  if ($m.Success) { $dt=[datetime]::MinValue; if ([datetime]::TryParseExact($m.Value,'yyyy-MM-dd',$culture,$styles,[ref]$dt)) { return $dt } }

  $m = [regex]::Match($s, '\b\d{1,2}/\d{1,2}/\d{4}\b')
  if ($m.Success) { $dt=[datetime]::MinValue; if ([datetime]::TryParse($m.Value,$culture,$styles,[ref]$dt)) { return $dt } }

  $dt2=[datetime]::MinValue
  if ([datetime]::TryParse($s,$culture,$styles,[ref]$dt2)) { return $dt2 }

  $m = [regex]::Match($s,'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$','IgnoreCase')
  if ($m.Success) {
    $first = ('{0} 1, {1}' -f $m.Groups[1].Value, $m.Groups[2].Value)
    $dt3=[datetime]::MinValue
    if ([datetime]::TryParse($first,$culture,$styles,[ref]$dt3)) { return $dt3 }
  }
  return $null
}

# Resolve month text/number to 1–12; returns $null if invalid
function Resolve-MonthNumber {
  param([Parameter(Mandatory)][string]$Month)
  $m = $Month.Trim()

  # Numeric: "7" or "07"
  $n = 0
  if ([int]::TryParse($m, [ref]$n)) {
    if ($n -ge 1 -and $n -le 12) { return $n } else { return $null }
  }

  # Names: "July", "Jul" (case-insensitive)
  $culture = [System.Globalization.CultureInfo]::GetCultureInfo('en-US')
  $title   = $culture.TextInfo.ToTitleCase($m.ToLowerInvariant())

  foreach ($fmt in 'MMMM','MMM') {
    try {
      $dt = [datetime]::ParseExact($title, $fmt, $culture)
      return $dt.Month
    } catch { }
  }

  return $null
}

# --- Dedup & shape helpers ---
function Select-OutputShape {
  param([Parameter(Mandatory)][object[]]$Rows)
  $Rows | Select-Object Title, Genre, Premiere, Network, ImdbRating
}

function Get-IdentityKey {
  param([Parameter(Mandatory)][psobject]$Row)
  if (-not $Row) { return $null }
  $t = if ($Row.Title)   { $Row.Title.Trim().ToLowerInvariant() }   else { '' }
  $n = if ($Row.Network) { $Row.Network.Trim().ToLowerInvariant() } else { '' }
  $pKey = ''
  if ($Row.Premiere) {
    $dt = Get-PremiereDate $Row.Premiere
    if ($dt) { $pKey = $dt.ToString('yyyy-MM-dd') }
    else     { $pKey = $Row.Premiere.Trim().ToLowerInvariant() }
  }
  return "$t|$n|$pKey"
}

function Dedup-ByKey {
  param([Parameter(Mandatory)][object[]]$Rows)
  $seen = New-Object 'System.Collections.Generic.HashSet[string]'
  $out = New-Object System.Collections.Generic.List[object]
  foreach ($r in $Rows) {
    if (-not $r) { continue }
    $k = Get-IdentityKey -Row $r
    if (-not $k) { continue }
    if (-not $seen.Contains($k)) {
      [void]$seen.Add($k)
      $out.Add($r)
    }
  }
  return $out.ToArray()
}

function Sort-ByImdbRatingDesc {
  param([Parameter(Mandatory)][object[]]$Rows)
  $Rows | Sort-Object -Property @{
    Expression = {
      $n = 0.0
      if ([double]::TryParse($_.ImdbRating, [ref]$n)) { $n } else { -1 }
    }
  } -Descending
}

# --- Main scrape across one or more URLs ---
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

$tableRe = [regex]::new('<table[^>]*class="[^"]*wikitable[^"]*"[^>]*>.*?<\/table>', 'IgnoreCase,Singleline')
$trRe    = [regex]::new('<tr[^>]*>.*?<\/tr>', 'IgnoreCase,Singleline')
$thRe    = [regex]::new('<th\b[^>]*>(.*?)<\/th>', 'IgnoreCase,Singleline')
$cellRe  = [regex]::new('(<td\b[^>]*>.*?<\/td>)|(<th\b[^>]*\bscope\s*=\s*["'']row["''][^>]*>.*?<\/th>)', 'IgnoreCase,Singleline')

$yearPattern    = "\b$Year\b"
$monthPattern   = '(?i)\b(January|Jan|February|Feb|March|Mar|April|Apr|May|June|Jun|July|Jul|August|Aug|September|Sept|Sep|October|Oct|November|Nov|December|Dec)\b'
$isoDatePattern = '\b\d{4}-\d{2}-\d{2}\b'
$usDatePattern  = '\b\d{1,2}/\d{1,2}/\d{4}\b'

# Compute optional month window
$windowStart = $null
$windowEndExclusive = $null
$useMonthWindow = $false

switch ($PSCmdlet.ParameterSetName) {
  'ExplicitMonth' {
    $mNum = Resolve-MonthNumber -Month $FilterMonth
    if (-not $mNum) { throw "FilterMonth '$FilterMonth' is not a valid month. Use 1..12, 'Jul', or 'July'." }
    $windowStart = Get-Date -Year $FilterYear -Month $mNum -Day 1
    $windowEndExclusive = $windowStart.AddMonths(1)
    $useMonthWindow = $true
  }
  'RelativeMonth' {
    $firstThisMonth = Get-Date -Day 1
    $windowStart = $firstThisMonth.AddMonths(-2)
    $windowEndExclusive = $windowStart.AddMonths(1)
    $useMonthWindow = $true
  }
  default { }
}

$imdbCache = @{}
$results = New-Object System.Collections.Generic.List[object]

foreach ($Url in $Urls) {
  # Download
  try {
    $resp = Invoke-Http -Uri $Url
    $html = $resp.Content
  } catch {
    Write-Warning ("Failed to download {0}: {1}" -f $Url, $_.Exception.Message)
    continue
  }

  # Determine network/service name
  $networkName = Get-NetworkFromUrlTitle -Url $Url
  if (-not $networkName) {
    if     ($Url -match '(?i)hbo[_\s]*max')                 { $networkName = 'HBO Max' }
    elseif ($Url -match '(?i)hbo')                          { $networkName = 'HBO' }
    elseif ($Url -match '(?i)netflix')                      { $networkName = 'Netflix' }
    elseif ($Url -match '(?i)paramount(\+|%2B)')            { $networkName = 'Paramount+' }
    elseif ($Url -match '(?i)disney(\+|%2B)')               { $networkName = 'Disney+' }
    elseif ($Url -match '(?i)apple.*tv(\+|%2B)')            { $networkName = 'Apple TV+' }
    elseif ($Url -match '(?i)hulu')                         { $networkName = 'Hulu' }
    elseif ($Url -match '(?i)peacock')                      { $networkName = 'Peacock' }
    elseif ($Url -match '(?i)amazon|prime\s*video')         { $networkName = 'Prime Video' }
    elseif ($Url -match '(?i)mgm(\+|%2B)')                  { $networkName = 'MGM+' }
    elseif ($Url -match '(?i)discovery(\+|%2B)')            { $networkName = 'Discovery+' }
    elseif ($Url -match '(?i)\bFX\b')                       { $networkName = 'FX' }
    elseif ($Url -match '(?i)\bAMC\b')                      { $networkName = 'AMC' }
    else                                                    { $networkName = 'Unknown' }
  }

  # Cutoff for Upcoming
  $cutoffIdx = Get-CutoffIndex -Html $html -Url $Url

  foreach ($t in $tableRe.Matches($html)) {
    if ($cutoffIdx -ge 0 -and $t.Index -ge $cutoffIdx) { continue }  # skip "Upcoming" and after

    $tableHtml = $t.Value

    # Header row
    $headerRow = $null
    foreach ($tr in $trRe.Matches($tableHtml)) {
      if ($tr.Value -match '<th\b' -and -not ($tr.Value -match 'scope\s*=\s*["'']row["'']')) { $headerRow = $tr.Value; break }
    }
    if (-not $headerRow) { continue }

    # Headers
    $headers = @()
    foreach ($m in $thRe.Matches($headerRow)) {
      $h = Remove-Html $m.Groups[1].Value
      if ($h) { $headers += $h }
    }
    if (-not $headers) { continue }

    # Column indexes
    $findIndex = {
      param($pattern)
      for ($i=0; $i -lt $headers.Count; $i++) { if ($headers[$i] -match $pattern) { return $i } }
      return -1
    }

    $idxTitle    = & $findIndex '^(?i)\s*(title|program(me)?|show)\s*$'
    $idxGenre    = & $findIndex '(?i)^\s*genre(s)?\s*$'
    $idxPremiere = & $findIndex '(?i)premiere(d)?|original\s*release|release\s*date|first\s*(aired|released)'
    if ($idxTitle -lt 0 -or $idxGenre -lt 0 -or $idxPremiere -lt 0) { continue }

    $highestIdx = [Math]::Max([Math]::Max($idxTitle, $idxGenre), $idxPremiere)

    foreach ($tr in $trRe.Matches($tableHtml)) {
      if ($tr.Value -match '<th\b' -and -not ($tr.Value -match 'scope\s*=\s*["'']row["'']')) { continue }

      # Cells (raw + cleaned)
      $rawCells = @()
      $cells = @()
      foreach ($cm in $cellRe.Matches($tr.Value)) {
        $innerRaw = $cm.Value -replace '^<td\b[^>]*>|^<th\b[^>]*>',''
        $innerRaw = $innerRaw -replace '</td>$|</th>$',''
        $rawCells += ,$innerRaw
        $cells    += ,(Remove-Html $innerRaw)
      }
      if (-not $cells -or $cells.Count -lt ($highestIdx + 1)) { continue }

      $title    = $cells[$idxTitle]
      $genre    = $cells[$idxGenre]
      $premiere = $cells[$idxPremiere]
      if ([string]::IsNullOrWhiteSpace($title) -or [string]::IsNullOrWhiteSpace($premiere)) { continue }

      # Title cell's Wikipedia href (first anchor)
      $titleHref = $null
      if ($idxTitle -lt $rawCells.Count) {
        $mHref = [regex]::Match($rawCells[$idxTitle], '<a[^>]+href="([^"#:]+)"', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        if ($mHref.Success) { $titleHref = $mHref.Groups[1].Value }
      }

      # Date filters
      $premDate = Get-PremiereDate $premiere

      if ($useMonthWindow) {
        if (-not $premDate) { continue }
        if ($premDate -lt $windowStart -or $premDate -ge $windowEndExclusive) { continue }
      } else {
        # Default behavior: year + month/date present
        $hasYear        = ($premiere -match $yearPattern)
        $hasMonthOrDate = ($premiere -match $monthPattern) -or ($premiere -match $isoDatePattern) -or ($premiere -match $usDatePattern)
        if (-not ($hasYear -and $hasMonthOrDate)) { continue }
      }

      # IMDb lookup (cached per-network+title)
      $premYear = if ($premDate) { $premDate.Year } else { Get-FirstYearFromText $premiere }
      $cacheKey = "$networkName|$title"
      if (-not $imdbCache.ContainsKey($cacheKey)) {
        $imdbCache[$cacheKey] = Get-ImdbAssessment -Title $title -PremiereYear $premYear -DelayMs $RequestDelayMs -TitleHref $titleHref -NetworkHint $networkName
      }
      $assessment = $imdbCache[$cacheKey]

      if ($assessment.Status -eq 'ok') {
        $meets = ($assessment.Rating -ge $MinRating -and $assessment.Votes -ge $MinVotes)
        if ($meets -or $IncludeBelowThreshold.IsPresent) {
          $results.Add([pscustomobject]@{
            Title      = $title
            Genre      = $genre
            Premiere   = $premiere
            Network    = $networkName
            ImdbRating = [math]::Round($assessment.Rating, 1)
          }) | Out-Null
        }
      }
      elseif ($assessment.Status -in @('not_found','error')) {
        $label = if ($assessment.Status -eq 'not_found') { 'IMDb not found' } else { 'IMDb lookup error' }
        $results.Add([pscustomobject]@{
          Title      = $title
          Genre      = $genre
          Premiere   = $premiere
          Network    = $networkName
          ImdbRating = $label
        }) | Out-Null
      }
    }
  }
}

# Shape and de-dup the current run (no pre-sort; sort happens at CSV write)
$runRows  = if ($results) { $results.ToArray() } else { @() }
$shapeNew = if ($runRows.Count -gt 0) { Select-OutputShape -Rows $runRows } else { @() }
$dedupNew = if ($shapeNew.Count -gt 0) { Dedup-ByKey -Rows $shapeNew } else { @() }

# Emit current run to console
$dedupNew

# --- Write to CSV (DEFAULT: merge + dedup + SORT ENTIRE FILE on write) ---
if ($OutputCsv) {
  $dir = Split-Path -Parent $OutputCsv
  if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }

  # New rows from this run (already shaped & de-duped earlier)
  $newRows = @($dedupNew)

  # Existing rows (if any)
  $existing = @()
  if (Test-Path $OutputCsv) {
    try   { $existing = @((Import-Csv $OutputCsv)) }
    catch { Write-Warning ("Failed to read existing CSV '{0}': {1}" -f $OutputCsv, $_.Exception.Message) }
  }
  $shapeExisting = if ($existing.Count -gt 0) { Select-OutputShape -Rows $existing } else { @() }

  # Build map (identity key → row). New rows override existing rows with the same key.
  $map = @{}
  foreach ($r in $shapeExisting) {
    if (-not $r) { continue }
    $k = Get-IdentityKey -Row $r
    if ($k) { $map[$k] = $r }
  }
  foreach ($r in $newRows) {
    if (-not $r) { continue }
    $k = Get-IdentityKey -Row $r
    if ($k) { $map[$k] = $r }
  }

  # Sort ENTIRE merged set by IMDb rating DESC (numeric first), then save
  $merged    = @($map.Values)
  $sortedAll = if ($merged.Count -gt 0) { Sort-ByImdbRatingDesc -Rows $merged } else { @() }

  if ($sortedAll.Count -gt 0) {
    $sortedAll | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutputCsv
  } else {
    # If nothing to write but file exists, leave it untouched; else create an empty CSV with headers.
    if (-not (Test-Path $OutputCsv)) {
      # write just headers
      "" | Select-Object @{n='Title';e={}}, @{n='Genre';e={}}, @{n='Premiere';e={}}, @{n='Network';e={}}, @{n='ImdbRating';e={}} |
        Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutputCsv
    }
  }

  Write-Host ("Merged {0} existing + {1} new → wrote {2} total row(s); fully resorted by rating DESC." -f $shapeExisting.Count, $newRows.Count, $sortedAll.Count)
}
