#requires -Version 7.2
<#
.SYNOPSIS
  Finds TV shows listed on Wikipedia network/programming pages that:
    - premiered in a selected year
    - have an IMDb rating at or above a selected threshold

.DESCRIPTION
  - Reads enabled Wikipedia list URLs from a CSV.
  - Extracts shows with exact premiere dates in the requested year.
  - Excludes future premiere dates by default.
  - Resolves each show's IMDb title ID through Wikipedia -> Wikidata (P345).
  - Downloads IMDb's official title.ratings.tsv.gz dataset for this run only.
  - Scans the ratings dataset once for the resolved IMDb IDs.
  - Writes qualifying shows to a CSV.

  No JSON caching, no merge/delta behavior, no IMDb HTML scraping,
  and no search-engine fallback matching.

.CONFIG CSV FORMAT
  URL,Enabled
  https://en.wikipedia.org/wiki/List_of_Netflix_original_programming,true
  https://en.wikipedia.org/wiki/List_of_HBO_original_programming,true
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateScript({ Test-Path -LiteralPath $_ -PathType Leaf })]
    [string]$ConfigCsv,

    [Parameter(Mandatory)]
    [ValidateRange(1900, 2200)]
    [int]$Year,

    [ValidateRange(0.0, 10.0)]
    [double]$MinRating = 8.4,

    [string]$OutputCsv,

    # Use a descriptive User-Agent with a real contact method.
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$UserAgent,

    # Optional: use a manually downloaded title.ratings.tsv.gz file instead
    # of downloading IMDb's current official ratings dataset for this run.
    [ValidateScript({
        if ([string]::IsNullOrWhiteSpace($_)) {
            return $true
        }

        Test-Path -LiteralPath $_ -PathType Leaf
    })]
    [string]$RatingsDatasetPath,

    # By default, exclude entries with premiere dates after today.
    [switch]$IncludeFuturePremieres,

    # Conservative sequential pacing for Wikipedia/Wikidata.
    [ValidateRange(0, 60000)]
    [int]$RequestDelayMs = 1500,

    [ValidateRange(0, 10)]
    [int]$MaxRetries = 6,

    # Smaller batches reduce Wikidata load.
    [ValidateRange(1, 50)]
    [int]$WikidataBatchSize = 10,

    [ValidateRange(5, 120)]
    [int]$TimeoutSeconds = 30,

    # Maximum compressed IMDb ratings dataset size allowed.
    [ValidateRange(25, 1024)]
    [int]$MaxDatasetSizeMB = 250
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:HttpClient = $null
$script:LastRequestUtc = $null
$script:TemporaryDatasetPath = $null
$script:InvariantCulture = [System.Globalization.CultureInfo]::InvariantCulture
$script:UsCulture = [System.Globalization.CultureInfo]::GetCultureInfo('en-US')

function New-ApiUri {
    param(
        [Parameter(Mandatory)]
        [string]$BaseUri,

        [Parameter(Mandatory)]
        [hashtable]$Parameters
    )

    $query = foreach ($key in $Parameters.Keys) {
        '{0}={1}' -f `
            [uri]::EscapeDataString([string]$key),
            [uri]::EscapeDataString([string]$Parameters[$key])
    }

    return [uri]::new(('{0}?{1}' -f $BaseUri, ($query -join '&')))
}

function Wait-ForRequestSlot {
    if ($null -eq $script:LastRequestUtc -or $RequestDelayMs -eq 0) {
        return
    }

    $elapsedMs = ([datetime]::UtcNow - $script:LastRequestUtc).TotalMilliseconds
    $waitMs = $RequestDelayMs - $elapsedMs

    if ($waitMs -gt 0) {
        Start-Sleep -Milliseconds ([int][math]::Ceiling($waitMs))
    }
}

function New-GetRequest {
    param(
        [Parameter(Mandatory)]
        [uri]$Uri,

        [Parameter(Mandatory)]
        [string]$Accept
    )

    $request = [System.Net.Http.HttpRequestMessage]::new(
        [System.Net.Http.HttpMethod]::Get,
        $Uri
    )

    [void]$request.Headers.TryAddWithoutValidation('User-Agent', $UserAgent)
    [void]$request.Headers.TryAddWithoutValidation('Accept', $Accept)
    [void]$request.Headers.TryAddWithoutValidation(
        'Accept-Language',
        'en-US,en;q=0.8'
    )

    return $request
}

function Get-RetryDelaySeconds {
    param(
        [Parameter(Mandatory)]
        [System.Net.Http.HttpResponseMessage]$Response,

        [Parameter(Mandatory)]
        [int]$Attempt
    )

    $retryAfter = $Response.Headers.RetryAfter

    if ($null -ne $retryAfter) {
        # PowerShell unwraps nullable .NET values, so do not use .HasValue.
        $retryAfterDelta = $retryAfter.Delta

        if ($null -ne $retryAfterDelta) {
            return [math]::Max(
                1,
                [int][math]::Ceiling($retryAfterDelta.TotalSeconds)
            )
        }

        $retryAfterDate = $retryAfter.Date

        if ($null -ne $retryAfterDate) {
            $seconds = (
                $retryAfterDate.UtcDateTime - [datetime]::UtcNow
            ).TotalSeconds

            return [math]::Max(
                1,
                [int][math]::Ceiling($seconds)
            )
        }
    }

    # Capped exponential backoff with minor jitter.
    $baseSeconds = [math]::Min(90, [math]::Pow(2, $Attempt + 2))
    $jitterSeconds = Get-Random -Minimum 0 -Maximum 4

    return [int]$baseSeconds + $jitterSeconds
}

function Invoke-GetText {
    param(
        [Parameter(Mandatory)]
        [uri]$Uri,

        [Parameter(Mandatory)]
        [string]$Accept
    )

    for ($attempt = 0; $attempt -le $MaxRetries; $attempt++) {
        $request = $null
        $response = $null
        $retryDelaySeconds = $null

        try {
            Wait-ForRequestSlot

            $request = New-GetRequest -Uri $Uri -Accept $Accept
            $script:LastRequestUtc = [datetime]::UtcNow

            $response = $script:HttpClient.SendAsync($request).GetAwaiter().GetResult()
            $statusCode = [int]$response.StatusCode

            if ($response.IsSuccessStatusCode) {
                return $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            }

            $isTransient = (
                $statusCode -eq 429 -or
                ($statusCode -ge 500 -and $statusCode -le 599)
            )

            if (-not $isTransient) {
                throw [System.InvalidOperationException]::new(
                    "HTTP $statusCode ($($response.ReasonPhrase)) for $($Uri.AbsoluteUri)"
                )
            }

            if ($attempt -eq $MaxRetries) {
                throw [System.InvalidOperationException]::new(
                    "HTTP $statusCode ($($response.ReasonPhrase)) for $($Uri.AbsoluteUri) " +
                    "after $($MaxRetries + 1) attempts."
                )
            }

            $retryDelaySeconds = Get-RetryDelaySeconds `
                -Response $response `
                -Attempt $attempt

            Write-Verbose (
                "HTTP $statusCode from $($Uri.Host). Retrying in " +
                "$retryDelaySeconds second(s)."
            )
        }
        catch [System.Net.Http.HttpRequestException] {
            if ($attempt -eq $MaxRetries) {
                throw
            }

            $retryDelaySeconds = [int][math]::Min(
                90,
                [math]::Pow(2, $attempt + 2)
            )

            Write-Verbose (
                "Network error from $($Uri.Host). Retrying in " +
                "$retryDelaySeconds second(s): $($_.Exception.Message)"
            )
        }
        catch [System.Threading.Tasks.TaskCanceledException] {
            if ($attempt -eq $MaxRetries) {
                throw
            }

            $retryDelaySeconds = [int][math]::Min(
                90,
                [math]::Pow(2, $attempt + 2)
            )

            Write-Verbose (
                "Timeout from $($Uri.Host). Retrying in " +
                "$retryDelaySeconds second(s): $($_.Exception.Message)"
            )
        }
        finally {
            if ($null -ne $response) {
                $response.Dispose()
            }

            if ($null -ne $request) {
                $request.Dispose()
            }
        }

        if ($null -ne $retryDelaySeconds) {
            Start-Sleep -Seconds $retryDelaySeconds
        }
    }

    throw "Request failed unexpectedly for $($Uri.AbsoluteUri)."
}

function Invoke-DownloadFile {
    param(
        [Parameter(Mandatory)]
        [uri]$Uri,

        [Parameter(Mandatory)]
        [string]$DestinationPath,

        [Parameter(Mandatory)]
        [int64]$MaxBytes
    )

    $partialPath = "$DestinationPath.partial"

    if (Test-Path -LiteralPath $partialPath) {
        Remove-Item -LiteralPath $partialPath -Force
    }

    try {
        for ($attempt = 0; $attempt -le $MaxRetries; $attempt++) {
            $request = $null
            $response = $null
            $retryDelaySeconds = $null

            try {
                Wait-ForRequestSlot

                $request = New-GetRequest `
                    -Uri $Uri `
                    -Accept 'application/gzip,application/octet-stream'

                $script:LastRequestUtc = [datetime]::UtcNow

                $response = $script:HttpClient.SendAsync(
                    $request,
                    [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead
                ).GetAwaiter().GetResult()

                $statusCode = [int]$response.StatusCode

                if ($response.IsSuccessStatusCode) {
                    $contentLength = $response.Content.Headers.ContentLength

                    # Do not use .HasValue or .Value here.
                    if (
                        $null -ne $contentLength -and
                        [int64]$contentLength -gt $MaxBytes
                    ) {
                        throw "IMDb ratings dataset exceeds the configured size limit."
                    }

                    $inputStream = $null
                    $outputStream = $null

                    try {
                        $inputStream = $response.Content.ReadAsStreamAsync().GetAwaiter().GetResult()

                        $outputStream = [System.IO.File]::Open(
                            $partialPath,
                            [System.IO.FileMode]::Create,
                            [System.IO.FileAccess]::Write,
                            [System.IO.FileShare]::None
                        )

                        $buffer = New-Object byte[] 131072
                        [int64]$totalBytes = 0

                        while ($true) {
                            $read = $inputStream.Read($buffer, 0, $buffer.Length)

                            if ($read -le 0) {
                                break
                            }

                            $totalBytes += $read

                            if ($totalBytes -gt $MaxBytes) {
                                throw "IMDb ratings dataset exceeds the configured size limit."
                            }

                            $outputStream.Write($buffer, 0, $read)
                        }

                        $outputStream.Flush()
                    }
                    finally {
                        if ($null -ne $outputStream) {
                            $outputStream.Dispose()
                        }

                        if ($null -ne $inputStream) {
                            $inputStream.Dispose()
                        }
                    }

                    Move-Item `
                        -LiteralPath $partialPath `
                        -Destination $DestinationPath `
                        -Force

                    return
                }

                $isTransient = (
                    $statusCode -eq 429 -or
                    ($statusCode -ge 500 -and $statusCode -le 599)
                )

                if (-not $isTransient) {
                    throw [System.InvalidOperationException]::new(
                        "HTTP $statusCode ($($response.ReasonPhrase)) for $($Uri.AbsoluteUri)"
                    )
                }

                if ($attempt -eq $MaxRetries) {
                    throw [System.InvalidOperationException]::new(
                        "HTTP $statusCode ($($response.ReasonPhrase)) for $($Uri.AbsoluteUri) " +
                        "after $($MaxRetries + 1) attempts."
                    )
                }

                $retryDelaySeconds = Get-RetryDelaySeconds `
                    -Response $response `
                    -Attempt $attempt

                Write-Verbose (
                    "HTTP $statusCode from $($Uri.Host). Retrying dataset download " +
                    "in $retryDelaySeconds second(s)."
                )
            }
            catch [System.Net.Http.HttpRequestException] {
                if ($attempt -eq $MaxRetries) {
                    throw
                }

                $retryDelaySeconds = [int][math]::Min(
                    90,
                    [math]::Pow(2, $attempt + 2)
                )
            }
            catch [System.Threading.Tasks.TaskCanceledException] {
                if ($attempt -eq $MaxRetries) {
                    throw
                }

                $retryDelaySeconds = [int][math]::Min(
                    90,
                    [math]::Pow(2, $attempt + 2)
                )
            }
            finally {
                if ($null -ne $response) {
                    $response.Dispose()
                }

                if ($null -ne $request) {
                    $request.Dispose()
                }
            }

            if ($null -ne $retryDelaySeconds) {
                Start-Sleep -Seconds $retryDelaySeconds
            }
        }
    }
    finally {
        if (Test-Path -LiteralPath $partialPath) {
            Remove-Item -LiteralPath $partialPath -Force -ErrorAction SilentlyContinue
        }
    }

    throw "Dataset download failed unexpectedly for $($Uri.AbsoluteUri)."
}

function Invoke-GetJson {
    param(
        [Parameter(Mandatory)]
        [uri]$Uri
    )

    $text = Invoke-GetText -Uri $Uri -Accept 'application/json'

    try {
        return $text | ConvertFrom-Json -Depth 100
    }
    catch {
        throw "Invalid JSON from $($Uri.AbsoluteUri): $($_.Exception.Message)"
    }
}

function Get-ObjectValue {
    param(
        [AllowNull()]
        [object]$Object,

        [Parameter(Mandatory)]
        [string]$Name
    )

    if ($null -eq $Object) {
        return $null
    }

    $property = $Object.PSObject.Properties[$Name]

    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function Get-NetworkNameFromListTitle {
    param([string]$ListTitle)

    $originalProgramming = [regex]::Match(
        $ListTitle,
        '^(?i)List of\s+(.+?)\s+original programming'
    )

    if ($originalProgramming.Success) {
        return $originalProgramming.Groups[1].Value.Trim()
    }

    $broadcastPrograms = [regex]::Match(
        $ListTitle,
        '^(?i)List of programs broadcast by\s+(.+)$'
    )

    if ($broadcastPrograms.Success) {
        return $broadcastPrograms.Groups[1].Value.Trim()
    }

    return $ListTitle
}

function Get-EnabledWikipediaPages {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    $rows = @(Import-Csv -LiteralPath $Path)

    if ($rows.Count -eq 0) {
        throw 'The config CSV is empty.'
    }

    $columnNames = @($rows[0].PSObject.Properties.Name)

    if (
        $columnNames -notcontains 'URL' -or
        $columnNames -notcontains 'Enabled'
    ) {
        throw 'ConfigCsv must contain URL and Enabled columns.'
    }

    $pages = [System.Collections.Generic.List[object]]::new()

    $seenTitles = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase
    )

    foreach ($row in $rows) {
        $enabledValue = ([string]$row.Enabled).Trim().ToLowerInvariant()

        if ($enabledValue -notin @('true', '1', 'yes', 'y', 'on')) {
            continue
        }

        $rawUrl = ([string]$row.URL).Trim()
        $rawUrl = $rawUrl.Trim([char[]]@([char]"'", [char]'"'))

        $uri = $null

        if (-not [uri]::TryCreate($rawUrl, [System.UriKind]::Absolute, [ref]$uri)) {
            Write-Warning "Skipping invalid URL: $rawUrl"
            continue
        }

        $isApprovedWikipediaUrl = (
            $uri.Scheme -ieq 'https' -and
            $uri.Host -ieq 'en.wikipedia.org' -and
            $uri.IsDefaultPort -and
            $uri.AbsolutePath.StartsWith(
                '/wiki/',
                [System.StringComparison]::OrdinalIgnoreCase
            )
        )

        if (-not $isApprovedWikipediaUrl) {
            Write-Warning "Skipping non-HTTPS en.wikipedia.org/wiki URL: $rawUrl"
            continue
        }

        $slug = $uri.AbsolutePath.Substring('/wiki/'.Length)
        $wikiTitle = ([uri]::UnescapeDataString($slug) -replace '_', ' ').Trim()

        if (
            [string]::IsNullOrWhiteSpace($wikiTitle) -or
            $wikiTitle -match '^(?i)(Special|File|Help|Category|Template|Portal):'
        ) {
            Write-Warning "Skipping non-article Wikipedia URL: $rawUrl"
            continue
        }

        if ($seenTitles.Add($wikiTitle)) {
            $pages.Add(
                [pscustomobject]@{
                    WikipediaListTitle = $wikiTitle
                    Network = Get-NetworkNameFromListTitle $wikiTitle
                }
            )
        }
    }

    if ($pages.Count -eq 0) {
        throw 'No enabled, valid Wikipedia URLs were found.'
    }

    return $pages.ToArray()
}

function ConvertFrom-HtmlText {
    param([string]$Html)

    if ([string]::IsNullOrWhiteSpace($Html)) {
        return ''
    }

    $text = [regex]::Replace($Html, '(?is)<sup\b[^>]*>.*?</sup>', '')
    $text = [regex]::Replace($text, '(?is)<br\s*/?\s*>', '; ')
    $text = [regex]::Replace($text, '(?is)<[^>]+>', '')
    $text = [System.Net.WebUtility]::HtmlDecode($text)
    $text = [regex]::Replace($text, '\[\d+\]|\s+', ' ')

    return $text.Trim()
}

function Get-FirstPremiereDate {
    param([string]$Text)

    $datePatterns = @(
        '\b\d{4}-\d{2}-\d{2}\b',
        '\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b',
        '\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b',
        '\b\d{1,2}/\d{1,2}/\d{4}\b'
    )

    foreach ($pattern in $datePatterns) {
        $match = [regex]::Match(
            $Text,
            $pattern,
            [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
        )

        if (-not $match.Success) {
            continue
        }

        $date = [datetime]::MinValue

        if (
            [datetime]::TryParse(
                $match.Value,
                $script:UsCulture,
                [System.Globalization.DateTimeStyles]::AllowWhiteSpaces,
                [ref]$date
            )
        ) {
            return $date.Date
        }
    }

    return $null
}

function Get-WikipediaTitleFromCell {
    param([string]$CellHtml)

    $link = [regex]::Match(
        $CellHtml,
        '(?is)<a\b[^>]*\bhref\s*=\s*["''](?<href>(?:/wiki/|\./)[^"''#?]+)[^"''#?]*["'']'
    )

    if (-not $link.Success) {
        return $null
    }

    $href = $link.Groups['href'].Value
    $slug = $href -replace '^(?:/wiki/|\./)', ''

    $title = ([uri]::UnescapeDataString($slug) -replace '_', ' ').Trim()

    if (
        [string]::IsNullOrWhiteSpace($title) -or
        $title -match '^(?i)(Special|File|Help|Category|Template|Portal):'
    ) {
        return $null
    }

    return $title
}

function Find-HeaderIndex {
    param(
        [Parameter(Mandatory)]
        [string[]]$Headers,

        [Parameter(Mandatory)]
        [string]$Pattern
    )

    for ($i = 0; $i -lt $Headers.Count; $i++) {
        if ($Headers[$i] -match $Pattern) {
            return $i
        }
    }

    return -1
}

function Get-ShowCandidates {
    param(
        [Parameter(Mandatory)]
        [object[]]$Pages,

        [Parameter(Mandatory)]
        [int]$TargetYear
    )

    $results = [System.Collections.Generic.List[object]]::new()
    [int]$missingArticleLinkCount = 0
    [int]$futurePremiereCount = 0

    $tablePattern = '(?is)<table\b(?=[^>]*\bclass\s*=\s*["''][^"'']*\bwikitable\b)[^>]*>.*?</table>'
    $rowPattern = '(?is)<tr\b[^>]*>.*?</tr>'
    $cellPattern = '(?is)<(?:td|th)\b[^>]*>(?<content>.*?)</(?:td|th)\s*>'

    foreach ($page in $Pages) {
        Write-Verbose "Reading Wikipedia page: $($page.WikipediaListTitle)"

        $parseUri = New-ApiUri 'https://en.wikipedia.org/w/api.php' @{
            action = 'parse'
            format = 'json'
            formatversion = '2'
            page = $page.WikipediaListTitle
            prop = 'text'
            disablelimitreport = '1'
            maxlag = '5'
        }

        try {
            $parseResponse = Invoke-GetJson -Uri $parseUri
            $parse = Get-ObjectValue -Object $parseResponse -Name 'parse'
            $html = [string](Get-ObjectValue -Object $parse -Name 'text')
        }
        catch {
            throw "Could not read '$($page.WikipediaListTitle)': $($_.Exception.Message)"
        }

        if ([string]::IsNullOrWhiteSpace($html)) {
            continue
        }

        $upcomingSection = [regex]::Match(
            $html,
            '(?i)id\s*=\s*["'']Upcoming(?:[_\s]+original)?[_\s]+programming["'']'
        )

        $upcomingCutoff = if ($upcomingSection.Success) {
            $upcomingSection.Index
        }
        else {
            [int]::MaxValue
        }

        foreach ($table in [regex]::Matches($html, $tablePattern)) {
            if ($table.Index -ge $upcomingCutoff) {
                continue
            }

            $rows = @([regex]::Matches($table.Value, $rowPattern))
            $headerRowIndex = -1
            $headers = @()

            for ($rowIndex = 0; $rowIndex -lt $rows.Count; $rowIndex++) {
                $rowHtml = $rows[$rowIndex].Value

                if (
                    $rowHtml -notmatch '(?is)<th\b' -or
                    $rowHtml -match '(?i)scope\s*=\s*["'']row["'']'
                ) {
                    continue
                }

                $headers = @(
                    [regex]::Matches($rowHtml, $cellPattern) |
                        ForEach-Object {
                            ConvertFrom-HtmlText $_.Groups['content'].Value
                        }
                )

                if ($headers.Count -gt 0) {
                    $headerRowIndex = $rowIndex
                    break
                }
            }

            if ($headerRowIndex -lt 0) {
                continue
            }

            $titleIndex = Find-HeaderIndex `
                -Headers $headers `
                -Pattern '^(?i)\s*(title|program(?:me)?|show)\s*$'

            $premiereIndex = Find-HeaderIndex `
                -Headers $headers `
                -Pattern '(?i)(original\s*release|premiere|first\s*(aired|released)|release\s*date)'

            $genreIndex = Find-HeaderIndex `
                -Headers $headers `
                -Pattern '^(?i)\s*genre(s)?\s*$'

            if ($titleIndex -lt 0 -or $premiereIndex -lt 0) {
                continue
            }

            for ($rowIndex = $headerRowIndex + 1; $rowIndex -lt $rows.Count; $rowIndex++) {
                $cells = @(
                    [regex]::Matches($rows[$rowIndex].Value, $cellPattern) |
                        ForEach-Object {
                            $_.Groups['content'].Value
                        }
                )

                if ($cells.Count -le [math]::Max($titleIndex, $premiereIndex)) {
                    continue
                }

                $title = ConvertFrom-HtmlText $cells[$titleIndex]
                $premiere = ConvertFrom-HtmlText $cells[$premiereIndex]

                if (
                    [string]::IsNullOrWhiteSpace($title) -or
                    [string]::IsNullOrWhiteSpace($premiere)
                ) {
                    continue
                }

                $premiereDate = Get-FirstPremiereDate $premiere

                if ($null -eq $premiereDate -or $premiereDate.Year -ne $TargetYear) {
                    continue
                }

                if (
                    -not $IncludeFuturePremieres.IsPresent -and
                    $premiereDate -gt (Get-Date).Date
                ) {
                    $futurePremiereCount++
                    continue
                }

                $articleTitle = Get-WikipediaTitleFromCell $cells[$titleIndex]

                if ([string]::IsNullOrWhiteSpace($articleTitle)) {
                    $missingArticleLinkCount++
                    continue
                }

                $genre = if ($genreIndex -ge 0 -and $cells.Count -gt $genreIndex) {
                    ConvertFrom-HtmlText $cells[$genreIndex]
                }
                else {
                    ''
                }

                $results.Add(
                    [pscustomobject]@{
                        Title = $title
                        Genre = $genre
                        Premiere = $premiere
                        PremiereDate = $premiereDate
                        Network = $page.Network
                        WikipediaTitle = $articleTitle
                    }
                )
            }
        }
    }

    Write-Verbose (
        "Candidate rows: $($results.Count). " +
        "Skipped without an English Wikipedia article link: $missingArticleLinkCount. " +
        "Skipped as future premieres: $futurePremiereCount."
    )

    return $results.ToArray()
}

function Get-WikidataQidMap {
    param(
        [Parameter(Mandatory)]
        [string[]]$WikipediaTitles
    )

    $result = @{}
    $titles = @($WikipediaTitles | Sort-Object -Unique)

    for ($offset = 0; $offset -lt $titles.Count; $offset += 50) {
        $lastIndex = [math]::Min($offset + 49, $titles.Count - 1)
        $batch = @($titles[$offset..$lastIndex])

        $uri = New-ApiUri 'https://en.wikipedia.org/w/api.php' @{
            action = 'query'
            format = 'json'
            redirects = '1'
            prop = 'pageprops'
            ppprop = 'wikibase_item'
            titles = ($batch -join '|')
            maxlag = '5'
        }

        try {
            $json = Invoke-GetJson -Uri $uri
        }
        catch {
            throw "Wikipedia-to-Wikidata lookup failed: $($_.Exception.Message)"
        }

        $query = Get-ObjectValue -Object $json -Name 'query'

        if ($null -eq $query) {
            throw 'Wikipedia API returned no query object for a title batch.'
        }

        $normalized = @{}
        $redirects = @{}
        $qidsByResolvedTitle = @{}

        foreach ($item in @(Get-ObjectValue -Object $query -Name 'normalized')) {
            $from = Get-ObjectValue -Object $item -Name 'from'
            $to = Get-ObjectValue -Object $item -Name 'to'

            if ($from -and $to) {
                $normalized[[string]$from] = [string]$to
            }
        }

        foreach ($item in @(Get-ObjectValue -Object $query -Name 'redirects')) {
            $from = Get-ObjectValue -Object $item -Name 'from'
            $to = Get-ObjectValue -Object $item -Name 'to'

            if ($from -and $to) {
                $redirects[[string]$from] = [string]$to
            }
        }

        $pages = Get-ObjectValue -Object $query -Name 'pages'

        if ($null -ne $pages) {
            foreach ($property in $pages.PSObject.Properties) {
                $page = $property.Value
                $pageTitle = Get-ObjectValue -Object $page -Name 'title'
                $pageProps = Get-ObjectValue -Object $page -Name 'pageprops'
                $qid = Get-ObjectValue -Object $pageProps -Name 'wikibase_item'

                if ($pageTitle -and $qid) {
                    $qidsByResolvedTitle[[string]$pageTitle] = [string]$qid
                }
            }
        }

        foreach ($originalTitle in $batch) {
            $resolvedTitle = if ($normalized.ContainsKey($originalTitle)) {
                $normalized[$originalTitle]
            }
            else {
                $originalTitle
            }

            if ($redirects.ContainsKey($resolvedTitle)) {
                $resolvedTitle = $redirects[$resolvedTitle]
            }

            if ($qidsByResolvedTitle.ContainsKey($resolvedTitle)) {
                $result[$originalTitle] = $qidsByResolvedTitle[$resolvedTitle]
            }
        }
    }

    return $result
}

function Get-ImdbIdMap {
    param(
        [Parameter(Mandatory)]
        [string[]]$Qids
    )

    $result = @{}

    $uniqueQids = @(
        $Qids |
            Where-Object { $_ -match '^Q\d+$' } |
            Sort-Object -Unique
    )

    for ($offset = 0; $offset -lt $uniqueQids.Count; $offset += $WikidataBatchSize) {
        $lastIndex = [math]::Min(
            $offset + $WikidataBatchSize - 1,
            $uniqueQids.Count - 1
        )

        $batch = @($uniqueQids[$offset..$lastIndex])

        $uri = New-ApiUri 'https://www.wikidata.org/w/api.php' @{
            action = 'wbgetentities'
            format = 'json'
            ids = ($batch -join '|')
            props = 'claims'
            maxlag = '5'
        }

        try {
            $json = Invoke-GetJson -Uri $uri
        }
        catch {
            throw (
                "Wikidata lookup failed after retries. " +
                "The output file was not changed. $($_.Exception.Message)"
            )
        }

        $entities = Get-ObjectValue -Object $json -Name 'entities'

        if ($null -eq $entities) {
            throw 'Wikidata returned no entities for an IMDb-ID lookup batch.'
        }

        foreach ($property in $entities.PSObject.Properties) {
            $entity = $property.Value
            $claimsObject = Get-ObjectValue -Object $entity -Name 'claims'
            $imdbClaims = Get-ObjectValue -Object $claimsObject -Name 'P345'

            foreach ($claim in @($imdbClaims)) {
                $mainSnak = Get-ObjectValue -Object $claim -Name 'mainsnak'
                $dataValue = Get-ObjectValue -Object $mainSnak -Name 'datavalue'
                $imdbId = [string](Get-ObjectValue -Object $dataValue -Name 'value')

                if ($imdbId -match '^tt\d+$') {
                    $result[$property.Name] = $imdbId
                    break
                }
            }
        }
    }

    return $result
}

function Get-RatingsDatasetFile {
    if (-not [string]::IsNullOrWhiteSpace($RatingsDatasetPath)) {
        return [System.IO.Path]::GetFullPath($RatingsDatasetPath)
    }

    $fileName = 'title.ratings.{0}.tsv.gz' -f (
        [guid]::NewGuid().ToString('N')
    )

    $temporaryPath = Join-Path `
        -Path ([System.IO.Path]::GetTempPath()) `
        -ChildPath $fileName

    $script:TemporaryDatasetPath = $temporaryPath

    Write-Verbose 'Downloading the official IMDb ratings dataset for this run.'

    Invoke-DownloadFile `
        -Uri ([uri]::new('https://datasets.imdbws.com/title.ratings.tsv.gz')) `
        -DestinationPath $temporaryPath `
        -MaxBytes ([int64]$MaxDatasetSizeMB * 1MB)

    return $temporaryPath
}

function Get-ImdbRatingsFromDataset {
    param(
        [Parameter(Mandatory)]
        [string[]]$ImdbIds,

        [Parameter(Mandatory)]
        [string]$DatasetPath
    )

    $neededIds = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )

    foreach ($imdbId in $ImdbIds) {
        if ($imdbId -match '^tt\d+$') {
            [void]$neededIds.Add($imdbId)
        }
    }

    $ratings = @{}

    if ($neededIds.Count -eq 0) {
        return $ratings
    }

    $fileStream = $null
    $gzipStream = $null
    $reader = $null

    try {
        $fileStream = [System.IO.File]::Open(
            $DatasetPath,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::Read
        )

        $gzipStream = [System.IO.Compression.GZipStream]::new(
            $fileStream,
            [System.IO.Compression.CompressionMode]::Decompress,
            $false
        )

        $reader = [System.IO.StreamReader]::new(
            $gzipStream,
            [System.Text.Encoding]::UTF8,
            $true,
            65536,
            $false
        )

        $header = $reader.ReadLine()

        if ($header -ne "tconst`taverageRating`tnumVotes") {
            throw 'The IMDb ratings file does not have the expected title.ratings TSV header.'
        }

        while ($null -ne ($line = $reader.ReadLine())) {
            $firstTab = $line.IndexOf("`t")

            if ($firstTab -lt 1) {
                continue
            }

            $secondTab = $line.IndexOf("`t", $firstTab + 1)

            if ($secondTab -lt 0) {
                continue
            }

            $imdbId = $line.Substring(0, $firstTab)

            if (-not $neededIds.Contains($imdbId)) {
                continue
            }

            [void]$neededIds.Remove($imdbId)

            $ratingText = $line.Substring(
                $firstTab + 1,
                $secondTab - $firstTab - 1
            )

            $votesText = $line.Substring($secondTab + 1)

            [double]$rating = 0
            [int64]$votes = 0

            $hasRating = [double]::TryParse(
                $ratingText,
                [System.Globalization.NumberStyles]::Float,
                $script:InvariantCulture,
                [ref]$rating
            )

            $hasVotes = [int64]::TryParse($votesText, [ref]$votes)

            if ($hasRating) {
                $ratings[$imdbId] = [pscustomobject]@{
                    Rating = $rating
                    Votes = if ($hasVotes) { $votes } else { $null }
                }
            }

            if ($neededIds.Count -eq 0) {
                break
            }
        }
    }
    finally {
        if ($null -ne $reader) {
            $reader.Dispose()
        }

        if ($null -ne $gzipStream) {
            $gzipStream.Dispose()
        }

        if ($null -ne $fileStream) {
            $fileStream.Dispose()
        }
    }

    Write-Verbose "IMDb ratings found: $($ratings.Count)."

    return $ratings
}

function Join-Unique {
    param([object[]]$Values)

    $unique = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase
    )

    foreach ($value in $Values) {
        $text = ([string]$value).Trim()

        if (-not [string]::IsNullOrWhiteSpace($text)) {
            [void]$unique.Add($text)
        }
    }

    return (@($unique | Sort-Object) -join '; ')
}

function Protect-CsvText {
    param([object]$Value)

    if ($Value -is [string] -and $Value -match '^[=+\-@]') {
        return "'$Value"
    }

    return $Value
}

function Export-Results {
    param(
        [Parameter(Mandatory)]
        [object[]]$Rows,

        [Parameter(Mandatory)]
        [string]$Path
    )

    $directory = Split-Path -Parent $Path

    if (-not [string]::IsNullOrWhiteSpace($directory)) {
        [void][System.IO.Directory]::CreateDirectory($directory)
    }

    if ($Rows.Count -eq 0) {
        Set-Content `
            -LiteralPath $Path `
            -Encoding utf8BOM `
            -Value 'Title,Genre,Premiere,Network,ImdbRating,ImdbVotes,ImdbId,WikidataQid'

        return
    }

    $Rows |
        ForEach-Object {
            [pscustomobject][ordered]@{
                Title       = Protect-CsvText $_.Title
                Genre       = Protect-CsvText $_.Genre
                Premiere    = Protect-CsvText $_.Premiere
                Network     = Protect-CsvText $_.Network
                ImdbRating  = $_.ImdbRating
                ImdbVotes   = $_.ImdbVotes
                ImdbId      = Protect-CsvText $_.ImdbId
                WikidataQid = Protect-CsvText $_.WikidataQid
            }
        } |
        Export-Csv `
            -LiteralPath $Path `
            -NoTypeInformation `
            -Encoding utf8BOM
}

try {
    if ([string]::IsNullOrWhiteSpace($OutputCsv)) {
        $ratingText = $MinRating.ToString('0.0', $script:InvariantCulture)

        $OutputCsv = Join-Path `
            -Path (Get-Location) `
            -ChildPath "TopRatedShows-$Year-IMDb$ratingText.csv"
    }

    $handler = [System.Net.Http.SocketsHttpHandler]::new()
    $handler.AllowAutoRedirect = $false
    $handler.UseCookies = $false
    $handler.AutomaticDecompression = (
        [System.Net.DecompressionMethods]::GZip -bor
        [System.Net.DecompressionMethods]::Deflate
    )

    $script:HttpClient = [System.Net.Http.HttpClient]::new($handler, $true)
    $script:HttpClient.Timeout = [timespan]::FromSeconds($TimeoutSeconds)
    $script:HttpClient.MaxResponseContentBufferSize = 10MB

    $pages = @(Get-EnabledWikipediaPages -Path $ConfigCsv)
    $candidates = @(Get-ShowCandidates -Pages $pages -TargetYear $Year)

    if ($candidates.Count -eq 0) {
        Export-Results -Rows @() -Path $OutputCsv
        Write-Host "No eligible Wikipedia rows were found for $Year. Wrote: $OutputCsv"
        return
    }

    $qidByWikipediaTitle = Get-WikidataQidMap `
        -WikipediaTitles @($candidates.WikipediaTitle)

    $imdbIdByQid = Get-ImdbIdMap `
        -Qids @($qidByWikipediaTitle.Values)

    $resolvedShows = @(
        foreach ($candidate in $candidates) {
            if (-not $qidByWikipediaTitle.ContainsKey($candidate.WikipediaTitle)) {
                continue
            }

            $qid = $qidByWikipediaTitle[$candidate.WikipediaTitle]

            if (-not $imdbIdByQid.ContainsKey($qid)) {
                continue
            }

            [pscustomobject]@{
                Title = $candidate.Title
                Genre = $candidate.Genre
                Premiere = $candidate.Premiere
                PremiereDate = $candidate.PremiereDate
                Network = $candidate.Network
                ImdbId = $imdbIdByQid[$qid]
                WikidataQid = $qid
            }
        }
    )

    Write-Verbose (
        "Candidates with a Wikidata-backed IMDb ID: $($resolvedShows.Count)."
    )

    if ($resolvedShows.Count -eq 0) {
        Export-Results -Rows @() -Path $OutputCsv
        Write-Host "No IMDb IDs could be resolved for $Year. Wrote: $OutputCsv"
        return
    }

    $datasetFile = Get-RatingsDatasetFile

    $ratingsByImdbId = Get-ImdbRatingsFromDataset `
        -ImdbIds @($resolvedShows.ImdbId | Sort-Object -Unique) `
        -DatasetPath $datasetFile

    $results = @(
        foreach ($group in @($resolvedShows | Group-Object ImdbId)) {
            $imdbId = [string]$group.Name

            if (-not $ratingsByImdbId.ContainsKey($imdbId)) {
                continue
            }

            $ratingInfo = $ratingsByImdbId[$imdbId]

            if ($ratingInfo.Rating -lt $MinRating) {
                continue
            }

            $showRows = @($group.Group)

            $firstRow = $showRows |
                Sort-Object PremiereDate, Title |
                Select-Object -First 1

            [pscustomobject]@{
                Title = $firstRow.Title
                Genre = Join-Unique $showRows.Genre
                Premiere = $firstRow.Premiere
                Network = Join-Unique $showRows.Network
                ImdbRating = [math]::Round(
                    [double]$ratingInfo.Rating,
                    1
                )
                ImdbVotes = $ratingInfo.Votes
                ImdbId = $imdbId
                WikidataQid = Join-Unique $showRows.WikidataQid
            }
        }
    )

    $sortedResults = @(
        $results |
            Sort-Object `
                @{ Expression = { $_.ImdbRating }; Descending = $true },
                @{ Expression = {
                    if ($null -ne $_.ImdbVotes) {
                        $_.ImdbVotes
                    }
                    else {
                        -1
                    }
                }; Descending = $true },
                Title
    )

    Export-Results -Rows $sortedResults -Path $OutputCsv

    Write-Host (
        "Found {0} show(s) released in {1} with IMDb rating >= {2}. Wrote: {3}" -f
        $sortedResults.Count,
        $Year,
        $MinRating.ToString('0.0', $script:InvariantCulture),
        $OutputCsv
    )
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
finally {
    if (
        -not [string]::IsNullOrWhiteSpace($script:TemporaryDatasetPath) -and
        (Test-Path -LiteralPath $script:TemporaryDatasetPath)
    ) {
        Remove-Item `
            -LiteralPath $script:TemporaryDatasetPath `
            -Force `
            -ErrorAction SilentlyContinue
    }

    if ($null -ne $script:HttpClient) {
        $script:HttpClient.Dispose()
    }
}