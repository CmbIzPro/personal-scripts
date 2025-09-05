<#
.SYNOPSIS
  Scrape Title, Genre, Premiere from pre-"Upcoming original programming" wikitables on:
  https://en.wikipedia.org/wiki/List_of_Netflix_original_programming

  Keep rows that:
    - have Premiere in the target Year (default: 2025) AND include a month/date (exclude year-only entries)
    - AND have IMDb rating >= MinRating AND votes >= MinVotes
    - OR encountered an IMDb lookup failure (included with ImdbRating = "IMDb not found" / "IMDb lookup error")

  Output is sorted by Premiere ascending.
#>

[CmdletBinding()]
param(
  [string]$Url = 'https://en.wikipedia.org/wiki/List_of_Netflix_original_programming',
  [string]$OutputCsv,
  [int]$Year = 2025,
  [double]$MinRating = 8.4,
  [int]$MinVotes = 10000,
  [int]$RequestDelayMs = 250
)

function Remove-Html {
  param([string]$Html)
  if ([string]::IsNullOrWhiteSpace($Html)) { return $null }
  $s = $Html
  $s = $s -replace '<sup[^>]*>.*?</sup>', ''                 # remove citation superscripts
  $s = $s -replace '<span[^>]*class="nowrap"[^>]*>', ''       # unwrap nowrap spans
  $s = $s -replace '<br\s*/?>', '; '                          # convert <br> to separators
  $s = $s -replace '<[^>]+>', ''                              # strip remaining tags
  $s = [System.Net.WebUtility]::HtmlDecode($s)                # decode entities
  $s = $s -replace '\[\d+\]', ''                              # remove [1] markers
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

# --- Wikidata helpers (NEW) ---
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
  param(
    [Parameter(Mandatory)][string]$EnwikiTitle,
    [int]$DelayMs = 250
  )
  try {
    $api = "https://en.wikipedia.org/w/api.php?action=query&format=json&prop=pageprops&ppprop=wikibase_item&titles=" +
            [System.Uri]::EscapeDataString($EnwikiTitle)
    Start-Sleep -Milliseconds $DelayMs
    $resp = Invoke-Http -Uri $api
    $j = $resp.Content | ConvertFrom-Json
    $page = ($j.query.pages.PSObject.Properties | Select-Object -First 1).Value
    if ($page -and $page.pageprops -and $page.pageprops.wikibase_item) {
      return $page.pageprops.wikibase_item
    }
  } catch { }
  return $null
}

function Get-ImdbIdFromWikidataQid {
  param(
    [Parameter(Mandatory)][string]$Qid,
    [int]$DelayMs = 250
  )
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
  } catch { }
  return $null
}

# --- IMDb helpers ---
function Try-GetImdbIdFromWikipediaPage {
  param(
    [Parameter(Mandatory)][string]$WikiHref,  # may be relative like /wiki/Show_Title
    [int]$DelayMs = 250
  )
  try {
    $uri = $WikiHref
    if ($uri -notmatch '^https?://') { $uri = 'https://en.wikipedia.org' + $WikiHref }
    Start-Sleep -Milliseconds $DelayMs
    $resp = Invoke-Http -Uri $uri
    $m = [regex]::Match($resp.Content, '/title/(tt\d+)', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($m.Success) { return $m.Groups[1].Value }
  } catch { }
  return $null
}

function Try-GetImdbIdFromWebSearch {
  param(
    [Parameter(Mandatory)][string]$Title,
    [int]$PremiereYear,
    [int]$DelayMs = 250
  )

  $queries = @()
  $clean = Clean-TitleForSearch $Title
  if ($PremiereYear) { $queries += "$clean ($PremiereYear) site:imdb.com/title" }
  $queries += "$clean Netflix site:imdb.com/title"
  $queries += "$clean site:imdb.com/title"

  foreach ($q in $queries) {
    $enc = [System.Uri]::EscapeDataString($q)
    foreach ($engine in @('bing','ddg')) {
      try {
        $url = if ($engine -eq 'bing') {
          "https://www.bing.com/search?q=$enc"
        } else {
          "https://duckduckgo.com/html/?q=$enc"
        }
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
  <#
    Given a tt-id, fetch title page and parse JSON-LD for aggregateRating.
    Returns @{ Status='ok'; Id; Rating; Votes } or @{ Status='not_found' } if rating block absent.
  #>
  param([Parameter(Mandatory)] [string]$ImdbId)

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
          if ($o.aggregateRating -and $o.aggregateRating.ratingValue -and $o.aggregateRating.ratingCount) {
            $best = $o; break
          }
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
    Returns one of:
      @{ Status='ok';       Id='tt....'; Rating=[double]; Votes=[int] }
      @{ Status='not_found' }
      @{ Status='error' }
    Tries, in order:
      1) IMDb suggestion
      2) IMDb find
      3) Wikidata P345 (via enwiki title or page link)
      4) Wikipedia page external IMDb link
      5) Web search (Bing, DuckDuckGo)
  #>
  param(
    [Parameter(Mandatory)] [string]$Title,
    [int]$PremiereYear,
    [int]$DelayMs = 250,
    [string]$TitleHref
  )

  try {
    # 1) IMDb suggestion API (original title, then cleaned title)
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
            [pscustomobject]@{ Id = $d.id; Score = $score }
          }
          if ($scored) {
            $ttId = ($scored | Sort-Object Score -Descending | Select-Object -First 1).Id
            if ($ttId) { Start-Sleep -Milliseconds $DelayMs; return Get-ImdbRating -ImdbId $ttId }
          }
        }
      } catch { }
    }

    # 2) IMDb "find" HTML search
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

    # 3) Wikidata P345 (preferred if available)
    $qid = $null
    if ($TitleHref) {
      $pageTitle = Get-WikiTitleFromHref $TitleHref
      if ($pageTitle) { $qid = Get-WikidataQidFromEnwikiTitle -EnwikiTitle $pageTitle -DelayMs $DelayMs }
    }
    if (-not $qid) {
      # try by plain page title (sometimes tables omit links)
      $qid = Get-WikidataQidFromEnwikiTitle -EnwikiTitle $Title -DelayMs $DelayMs
    }
    if ($qid) {
      $tt2 = Get-ImdbIdFromWikidataQid -Qid $qid -DelayMs $DelayMs
      if ($tt2) { Start-Sleep -Milliseconds $DelayMs; return Get-ImdbRating -ImdbId $tt2 }
    }

    # 4) Wikipedia article page -> external IMDb link
    if ($TitleHref) {
      $tt3 = Try-GetImdbIdFromWikipediaPage -WikiHref $TitleHref -DelayMs $DelayMs
      if ($tt3) { Start-Sleep -Milliseconds $DelayMs; return Get-ImdbRating -ImdbId $tt3 }
    }

    # 5) Web search (Bing -> DuckDuckGo) for a tt-id
    $tt4 = Try-GetImdbIdFromWebSearch -Title $Title -PremiereYear $PremiereYear -DelayMs $DelayMs
    if ($tt4) { Start-Sleep -Milliseconds $DelayMs; return Get-ImdbRating -ImdbId $tt4 }

    return @{ Status='not_found' }
  }
  catch {
    return @{ Status='error' }
  }
}

# --- Helper to parse Premiere into a sortable DateTime key ---
function Get-PremiereSortKey {
  param([string]$Premiere)

  # Put unparseable at the end
  $fallback = [DateTime]::MaxValue

  if ([string]::IsNullOrWhiteSpace($Premiere)) { return $fallback }

  # Use first segment if multiple dates (we turned <br> into '; ')
  $s = ($Premiere -split ';')[0].Trim()

  # If there is a range with en/em dash (e.g., "Jan 5–7, 2025"), keep the first part
  $s = ($s -split '[–—]')[0].Trim()

  # Try common explicit patterns first
  $culture = [System.Globalization.CultureInfo]::GetCultureInfo('en-US')
  $styles  = [System.Globalization.DateTimeStyles]::AssumeLocal

  # ISO yyyy-MM-dd
  $m = [regex]::Match($s, '\b\d{4}-\d{2}-\d{2}\b')
  if ($m.Success) {
    $dt = [datetime]::MinValue
    if ([datetime]::TryParseExact($m.Value, 'yyyy-MM-dd', $culture, $styles, [ref]$dt)) { return $dt }
  }

  # US M/d/yyyy
  $m = [regex]::Match($s, '\b\d{1,2}/\d{1,2}/\d{4}\b')
  if ($m.Success) {
    $dt = [datetime]::MinValue
    if ([datetime]::TryParse($m.Value, $culture, $styles, [ref]$dt)) { return $dt }
  }

  # Month d, yyyy  OR  d Month yyyy  OR general natural-language date
  $dt2 = [datetime]::MinValue
  if ([datetime]::TryParse($s, $culture, $styles, [ref]$dt2)) { return $dt2 }

  # Month yyyy (no day) -> assume day 1
  $m = [regex]::Match($s, '^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$', 'IgnoreCase')
  if ($m.Success) {
    $first = ('{0} 1, {1}' -f $m.Groups[1].Value, $m.Groups[2].Value)
    $dt3 = [datetime]::MinValue
    if ([datetime]::TryParse($first, $culture, $styles, [ref]$dt3)) { return $dt3 }
  }

  return $fallback
}

# --- Download the Wikipedia HTML ---
try {
  try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}
  $resp = Invoke-Http -Uri $Url
  $html = $resp.Content
}
catch {
  throw "Failed to download $Url. $_"
}

# --- Regexes for Wikipedia tables and headers ---
$tableRe = [regex]::new('<table[^>]*class="[^"]*wikitable[^"]*"[^>]*>.*?<\/table>', 'IgnoreCase,Singleline')
$trRe    = [regex]::new('<tr[^>]*>.*?<\/tr>', 'IgnoreCase,Singleline')
$thRe    = [regex]::new('<th\b[^>]*>(.*?)<\/th>', 'IgnoreCase,Singleline')
$cellRe  = [regex]::new('(<td\b[^>]*>.*?<\/td>)|(<th\b[^>]*\bscope\s*=\s*["'']row["''][^>]*>.*?<\/th>)', 'IgnoreCase,Singleline')

# --- Find cutoff index for "Upcoming original programming" (or fallback to "The Abandons") ---
$cutoffIdx = -1
$upcomingIdRe   = [regex]::new('id\s*=\s*["'']Upcoming[_\s]original[_\s]programming["'']', 'IgnoreCase')
$u = $upcomingIdRe.Match($html)
if (-not $u.Success) {
  $upcomingTextRe = [regex]::new('>\s*Upcoming\s+original\s+programming\s*<', 'IgnoreCase')
  $u = $upcomingTextRe.Match($html)
}
if ($u.Success) {
  $cutoffIdx = $u.Index
} else {
  $abMatches = [regex]::Matches($html, '(?i)The\s+Abandons')
  if ($abMatches.Count -gt 0) { $cutoffIdx = $abMatches[$abMatches.Count-1].Index }
}

# Patterns for year/month filtering
$yearPattern    = "\b$Year\b"
$monthPattern   = '(?i)\b(January|Jan|February|Feb|March|Mar|April|Apr|May|June|Jun|July|Jul|August|Aug|September|Sept|Sep|October|Oct|November|Nov|December|Dec)\b'
$isoDatePattern = '\b\d{4}-\d{2}-\d{2}\b'
$usDatePattern  = '\b\d{1,2}/\d{1,2}/\d{4}\b'

# Caches
$imdbCache = @{}
$results = New-Object System.Collections.Generic.List[object]

foreach ($t in $tableRe.Matches($html)) {
  if ($cutoffIdx -ge 0 -and $t.Index -ge $cutoffIdx) { continue }  # skip "Upcoming" and after

  $tableHtml = $t.Value

  # Identify header row
  $headerRow = $null
  foreach ($tr in $trRe.Matches($tableHtml)) {
    if ($tr.Value -match '<th\b' -and -not ($tr.Value -match 'scope\s*=\s*["'']row["'']')) { $headerRow = $tr.Value; break }
  }
  if (-not $headerRow) { continue }

  # Extract headers
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

    # Grab Title cell's Wikipedia href (first anchor)
    $titleHref = $null
    if ($idxTitle -lt $rawCells.Count) {
      $mHref = [regex]::Match($rawCells[$idxTitle], '<a[^>]+href="([^"#:]+)"', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
      if ($mHref.Success) { $titleHref = $mHref.Groups[1].Value }
    }

    # Wikipedia-side filter: year + month/date required
    $hasYear        = ($premiere -match $yearPattern)
    $hasMonthOrDate = ($premiere -match $monthPattern) -or ($premiere -match $isoDatePattern) -or ($premiere -match $usDatePattern)
    if (-not ($hasYear -and $hasMonthOrDate)) { continue }

    # IMDb lookup (cached by Title)
    $premYear = Get-FirstYearFromText $premiere
    if (-not $imdbCache.ContainsKey($title)) {
      $imdbCache[$title] = Get-ImdbAssessment -Title $title -PremiereYear $premYear -DelayMs $RequestDelayMs -TitleHref $titleHref
    }
    $assessment = $imdbCache[$title]

    if ($assessment.Status -eq 'ok') {
      if ($assessment.Rating -ge $MinRating -and $assessment.Votes -ge $MinVotes) {
        $results.Add([pscustomobject]@{
          Title      = $title
          Genre      = $genre
          Premiere   = $premiere
          ImdbRating = [math]::Round($assessment.Rating, 1)
        }) | Out-Null
      }
    }
    elseif ($assessment.Status -in @('not_found','error')) {
      # Include not-found/error rows with an explanatory ImdbRating value
      $label = if ($assessment.Status -eq 'not_found') { 'IMDb not found' } else { 'IMDb lookup error' }
      $results.Add([pscustomobject]@{
        Title      = $title
        Genre      = $genre
        Premiere   = $premiere
        ImdbRating = $label
      }) | Out-Null
    }
  }
}

# --- Sort by Premiere ascending ---
$sorted = $results | Sort-Object @{ Expression = { Get-PremiereSortKey $_.Premiere } }, @{ Expression = 'Title' }

# Emit results and optionally save
$sorted

if ($OutputCsv) {
  $dir = Split-Path -Parent $OutputCsv
  if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
  $sorted | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutputCsv
  Write-Host "Saved $($sorted.Count) rows to $OutputCsv"
}