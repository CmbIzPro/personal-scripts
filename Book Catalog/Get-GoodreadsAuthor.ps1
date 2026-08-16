<#
.SYNOPSIS
    Builds a filtered, ranked, and optionally scheduled reading plan from Goodreads author-list URLs.

.DESCRIPTION
    Reads a comma- or tab-delimited file with URL and Genre columns, optionally skips
    rows marked Y in an Exclude column, and force-includes rows marked Y in an Include
    column. Force-included authors bypass ratings-based pagination and book/series
    thresholds, but all non-rating eligibility and safety rules still apply. The script
    crawls each remaining Goodreads author list, filters standalone books and series,
    ranks authors using a configurable genre rotation, and exports one CSV reading plan.
    By default, individual book pages are requested only for plausible survivors.
    AuthorListOnly prevents all individual book-page requests.
    Goodreads responses are not cached or retained between runs.
    Duplicate displayed titles for the same author are reduced to the copy with the
    highest Goodreads ratings count.
    A book must contain a valid "published YYYY" value on the author-list page; rows
    without a publication year are excluded before any standalone or series calculation.

    Important interpretation: the output column named "Review count" contains the
    Goodreads ratings count. This is also the count used by MinimumRatingsCount.

    The script uses public HTML only. It does not sign in, bypass access controls, or
    require a Goodreads API key. Goodreads can change its HTML at any time; parsing
    failures are recorded in the log.

.PARAMETER InputCsv
    Input CSV/TSV containing the required URL and Genre columns. An optional Exclude
    column skips a row before URL validation or any Goodreads request when its trimmed
    value is Y (case-insensitive). An optional Include column force-includes all otherwise
    eligible books for that author without applying rating, ratings-count, series-average,
    or ratings-based pagination cutoffs. Include does not override missing publication
    years, contributor-only records, adaptations, omnibus/collection exclusions,
    duplicate handling, or required page counts. A row cannot have Y in both columns.
    An optional Author column is used only to make log messages easier to identify. Other
    columns are ignored. Plain URLs and Markdown-style [text](URL) values are accepted.

.PARAMETER OutputCsv
    Destination CSV. Defaults to Goodreads-Reading-Plan.csv in the current directory.

.PARAMETER Order
    Author interleaving order. Accepted values include "Order 1" through "Order 4",
    "Order1" through "Order4", and 1 through 4.

.PARAMETER StartDate
    Candidate start date for the first book. Friday and Saturday values advance to
    Sunday. Sunday through Thursday are used as supplied. Defaults to today. Ignored
    when AuthorListOnly is used.

.PARAMETER MinimumRating
    Minimum standalone rating and minimum unweighted series average. Defaults to 4.0.

.PARAMETER MinimumRatingsCount
    Minimum Goodreads ratings count for a standalone or the #1 book of a series.
    Defaults to 1000. The current author-list page is processed completely, but the
    script does not request its next page when any listed book has fewer ratings than
    this value.

.PARAMETER PagesPerReadingDay
    Pages assigned to each Sunday-through-Thursday reading day. Defaults to 20. Ignored
    when AuthorListOnly is used.

.PARAMETER ScheduleCutoffYear
    The first year for which no dates are assigned. The qualifying book and every
    subsequent book receive blank dates. Defaults to the execution year plus two.
    Ignored when AuthorListOnly is used.

.PARAMETER MaxConcurrency
    Maximum concurrent Goodreads book-detail requests. Defaults to 3 and is capped at 4.
    Ignored when AuthorListOnly is used.

.PARAMETER RequestDelayMs
    Minimum spacing between request starts. Defaults to 1000 milliseconds.

.PARAMETER LogPath
    Log-file path. Defaults to the output path with a .log extension.

.PARAMETER AuthorListOnly
    Never requests individual Goodreads book pages. Series information is taken only
    from author-list titles, while Pages and both estimated reading-date columns are
    left blank. This is the fastest mode.

.PARAMETER FullMetadataScan
    Opens every candidate book detail page before filtering. By default, the script
    first rejects clearly ineligible standalones and series using the lighter author
    list pages. Use this switch if an unusual Goodreads record omits or mislabels its
    series on the author list page. It cannot be combined with AuthorListOnly.

.EXAMPLE
    ./Get-GoodreadsReadingPlan.ps1 `
        -InputCsv ./authors.csv `
        -OutputCsv ./reading-plan.csv `
        -Order 'Order 1' `
        -Verbose

.EXAMPLE
    ./Get-GoodreadsReadingPlan.ps1 `
        -InputCsv ./authors.tsv `
        -Order 4 `
        -AuthorListOnly

.NOTES
    Target runtime: PowerShell 7.2 or later.
#>

[System.Diagnostics.CodeAnalysis.SuppressMessageAttribute(
    'PSReviewUnusedParameter',
    '',
    Justification = 'Script parameters are consumed by private helper functions through script scope.'
)]
[System.Diagnostics.CodeAnalysis.SuppressMessageAttribute(
    'PSUseShouldProcessForStateChangingFunctions',
    '',
    Justification = 'Private helpers construct HTTP objects or perform writes explicitly requested by the script invocation.'
)]
[CmdletBinding()]
param(
    [Parameter(Mandatory, Position = 0)]
    [ValidateScript({
        if (-not (Test-Path -LiteralPath $_ -PathType Leaf)) {
            throw "Input file does not exist: $_"
        }
        $true
    })]
    [Alias('ConfigCsv')]
    [string]$InputCsv,

    [Parameter(Position = 1)]
    [string]$OutputCsv = (Join-Path -Path (Get-Location) -ChildPath 'Goodreads-Reading-Plan.csv'),

    [ValidateSet('1', '2', '3', '4', 'Order1', 'Order2', 'Order3', 'Order4',
        'Order 1', 'Order 2', 'Order 3', 'Order 4', IgnoreCase = $true)]
    [string]$Order = 'Order 1',

    [datetime]$StartDate = (Get-Date).Date,

    [ValidateRange(0.0, 5.0)]
    [double]$MinimumRating = 4.0,

    [ValidateRange(0, [int]::MaxValue)]
    [int]$MinimumRatingsCount = 1000,

    [ValidateRange(1, 10000)]
    [int]$PagesPerReadingDay = 20,

    [ValidateRange(2000, 9999)]
    [int]$ScheduleCutoffYear = ((Get-Date).Year + 2),

    [ValidateRange(1, 4)]
    [int]$MaxConcurrency = 3,

    [ValidateRange(250, 30000)]
    [int]$RequestDelayMs = 1000,

    [ValidateRange(0, 20)]
    [int]$MaxRetries = 5,

    [ValidateRange(5, 300)]
    [int]$RequestTimeoutSeconds = 45,

    [string]$LogPath,

    [ValidateLength(10, 512)]
    [string]$UserAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) GoodreadsReadingPlanner/1.0',

    [Alias('SkipBookPages', 'NoBookPages')]
    [switch]$AuthorListOnly,

    [switch]$FullMetadataScan
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$script:InvariantCulture = [System.Globalization.CultureInfo]::InvariantCulture
$script:Utf8NoBom = [System.Text.UTF8Encoding]::new($false)
$script:HttpClient = $null
$script:HttpHandler = $null
$script:LastRequestStartUtc = [datetime]::MinValue
$script:CurrentDelayMs = $RequestDelayMs
$script:SuccessesSinceThrottle = 0
$script:ExclusionCounts = @{}

function Get-UnresolvedFullPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    return $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Path)
}

function Write-RunLog {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateSet('DEBUG', 'INFO', 'WARN', 'ERROR')]
        [string]$Level,

        [Parameter(Mandatory)]
        [AllowEmptyString()]
        [string]$Message
    )

    $safeMessage = $Message -replace '[\r\n]+', ' '
    $line = '{0} [{1}] {2}{3}' -f (
        [datetime]::Now.ToString('yyyy-MM-dd HH:mm:ss.fff zzz', $script:InvariantCulture),
        $Level,
        $safeMessage,
        [Environment]::NewLine
    )

    [System.IO.File]::AppendAllText($script:LogPath, $line, $script:Utf8NoBom)

    switch ($Level) {
        'DEBUG' { Write-Verbose $safeMessage }
        'INFO'  { Write-Verbose $safeMessage }
        'WARN'  { Write-Warning $safeMessage }
        'ERROR' { Write-Warning $safeMessage }
    }
}

function Add-Exclusion {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Reason,

        [string]$Author,

        [string]$Title
    )

    if (-not $script:ExclusionCounts.ContainsKey($Reason)) {
        $script:ExclusionCounts[$Reason] = 0
    }
    $script:ExclusionCounts[$Reason]++

    $context = @($Author, $Title) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    if ($context.Count -gt 0) {
        Write-RunLog -Level DEBUG -Message ("Excluded [{0}]: {1}" -f $Reason, ($context -join ' - '))
    }
}

function ConvertFrom-HtmlText {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ''
    }

    $withoutTags = [regex]::Replace($Value, '(?is)<!--.*?-->|<[^>]+>', ' ')
    $decoded = [System.Net.WebUtility]::HtmlDecode($withoutTags)
    return ([regex]::Replace($decoded, '\s+', ' ')).Trim()
}

function Get-ObjectPropertyValue {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$InputObject,

        [Parameter(Mandatory)]
        [string]$Name
    )

    if ($null -eq $InputObject) {
        return $null
    }

    $property = $InputObject.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function ConvertTo-InvariantNumber {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Value,

        [Parameter(Mandatory)]
        [ValidateSet('Double', 'Int64')]
        [string]$Type
    )

    $clean = $Value.Replace(',', '').Trim()
    if ($Type -eq 'Double') {
        $parsedDouble = 0.0
        if ([double]::TryParse(
                $clean,
                [System.Globalization.NumberStyles]::Float,
                $script:InvariantCulture,
                [ref]$parsedDouble)) {
            return $parsedDouble
        }
    }
    else {
        $parsedInt = [long]0
        if ([long]::TryParse(
                $clean,
                [System.Globalization.NumberStyles]::Integer,
                $script:InvariantCulture,
                [ref]$parsedInt)) {
            return $parsedInt
        }
    }

    throw "Could not parse numeric value '$Value'."
}

function Format-Decimal {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value,

        [ValidateRange(0, 10)]
        [int]$MaximumDecimals = 3
    )

    if ($null -eq $Value) {
        return ''
    }

    $format = if ($MaximumDecimals -eq 0) {
        '0'
    }
    else {
        '0.' + ('#' * $MaximumDecimals)
    }

    return ([double]$Value).ToString($format, $script:InvariantCulture)
}

function Protect-CsvText {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Value
    )

    if ([string]::IsNullOrEmpty($Value)) {
        return ''
    }

    # Prevent spreadsheet programs from evaluating scraped text as a formula.
    if ($Value -match '^[=+\-@]') {
        return "'$Value"
    }

    return $Value
}

function ConvertTo-NormalizedAuthorUrl {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Value
    )

    $candidate = $Value.Trim()
    $markdownMatch = [regex]::Match($candidate, '^\[[^\]]*\]\((?<url>https?://[^)]+)\)$', 'IgnoreCase')
    if ($markdownMatch.Success) {
        $candidate = $markdownMatch.Groups['url'].Value
    }

    $candidate = $candidate.Replace('\_', '_')
    $uri = $null
    if (-not [uri]::TryCreate($candidate, [System.UriKind]::Absolute, [ref]$uri)) {
        throw "Invalid Goodreads URL: '$Value'."
    }

    if ($uri.Scheme -ne 'https' -or $uri.Host -notin @('goodreads.com', 'www.goodreads.com')) {
        throw "Only HTTPS Goodreads URLs are allowed: '$Value'."
    }

    $pathMatch = [regex]::Match(
        $uri.AbsolutePath,
        '^/author/list/(?<id>\d+)(?:\.[A-Za-z0-9._~%\-]+)?/?$',
        'IgnoreCase'
    )
    if (-not $pathMatch.Success) {
        throw "Expected a Goodreads /author/list/<id> URL, received '$Value'."
    }

    $authorId = $pathMatch.Groups['id'].Value
    $slug = $uri.AbsolutePath.TrimEnd('/').Split('/')[-1]
    return [pscustomobject]@{
        AuthorId = $authorId
        Url      = "https://www.goodreads.com/author/list/$slug"
    }
}

function Test-AllowedGoodreadsUri {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [uri]$Uri
    )

    if (-not $Uri.IsAbsoluteUri -or $Uri.Scheme -ne 'https') {
        return $false
    }

    if ($Uri.Host -notin @('goodreads.com', 'www.goodreads.com')) {
        return $false
    }

    return $Uri.AbsolutePath -match '^/(?:author/list|book/show)/'
}

function Resolve-GoodreadsRedirect {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [uri]$CurrentUri,

        [Parameter(Mandatory)]
        [uri]$Location
    )

    $resolved = if ($Location.IsAbsoluteUri) {
        $Location
    }
    else {
        [uri]::new($CurrentUri, $Location)
    }

    if (-not (Test-AllowedGoodreadsUri -Uri $resolved)) {
        throw "Goodreads redirected to a disallowed URI: '$resolved'."
    }

    return $resolved
}

function Wait-ForRequestSlot {
    [CmdletBinding()]
    param()

    if ($script:LastRequestStartUtc -ne [datetime]::MinValue) {
        $elapsedMs = ([datetime]::UtcNow - $script:LastRequestStartUtc).TotalMilliseconds
        $remainingMs = $script:CurrentDelayMs - $elapsedMs
        if ($remainingMs -gt 0) {
            Start-Sleep -Milliseconds ([int][math]::Ceiling($remainingMs))
        }
    }

    $script:LastRequestStartUtc = [datetime]::UtcNow
}

function Test-GoodreadsChallengeResponse {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [System.Net.Http.HttpResponseMessage]$Response
    )

    if ($null -eq $Response) {
        return $false
    }

    $values = $null
    $hasChallengeHeader = $Response.Headers.TryGetValues('x-amzn-waf-action', [ref]$values) -and
        (@($values) -contains 'challenge')
    return $hasChallengeHeader -or ([int]$Response.StatusCode -eq 202 -and $Response.Content.Headers.ContentLength -eq 0)
}

function Register-GoodreadsThrottle {
    [CmdletBinding()]
    param()

    $script:CurrentDelayMs = [math]::Min(10000, [math]::Max(3000, $script:CurrentDelayMs * 2))
    $script:SuccessesSinceThrottle = 0
    Write-RunLog -Level WARN -Message "Goodreads requested slower traffic; adaptive request spacing is now $($script:CurrentDelayMs) ms."
}

function Register-GoodreadsSuccess {
    [CmdletBinding()]
    param()

    $script:SuccessesSinceThrottle++
    if ($script:SuccessesSinceThrottle -ge 10 -and $script:CurrentDelayMs -gt $RequestDelayMs) {
        $script:CurrentDelayMs = [math]::Max($RequestDelayMs, $script:CurrentDelayMs - 500)
        $script:SuccessesSinceThrottle = 0
        Write-RunLog -Level DEBUG -Message "Adaptive request spacing reduced to $($script:CurrentDelayMs) ms."
    }
}

function Get-RetryDelay {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [int]$Attempt,

        [AllowNull()]
        [System.Net.Http.HttpResponseMessage]$Response
    )

    if (Test-GoodreadsChallengeResponse -Response $Response) {
        $challengeDelay = 10000 * [math]::Pow(2, [math]::Min([math]::Max($Attempt - 1, 0), 3))
        return [int][math]::Min(60000, $challengeDelay + [System.Random]::Shared.Next(500, 2001))
    }

    if ($null -ne $Response -and $null -ne $Response.Headers.RetryAfter) {
        if ($null -ne $Response.Headers.RetryAfter.Delta) {
            return [int][math]::Min(30000, [math]::Max(250, $Response.Headers.RetryAfter.Delta.TotalMilliseconds))
        }

        if ($null -ne $Response.Headers.RetryAfter.Date) {
            $retryMs = ($Response.Headers.RetryAfter.Date.UtcDateTime - [datetime]::UtcNow).TotalMilliseconds
            return [int][math]::Min(30000, [math]::Max(250, $retryMs))
        }
    }

    $base = [math]::Min(30000, 500 * [math]::Pow(2, [math]::Min($Attempt, 6)))
    return [int][math]::Min(30000, $base + [System.Random]::Shared.Next(100, 501))
}

function New-GoodreadsHttpClient {
    [CmdletBinding()]
    param()

    $handler = [System.Net.Http.HttpClientHandler]::new()
    $handler.AllowAutoRedirect = $false
    $handler.UseCookies = $true
    $handler.CookieContainer = [System.Net.CookieContainer]::new()
    $handler.MaxConnectionsPerServer = $MaxConcurrency

    $decompression = [System.Net.DecompressionMethods]::GZip -bor [System.Net.DecompressionMethods]::Deflate
    if ([enum]::GetNames([System.Net.DecompressionMethods]) -contains 'Brotli') {
        $decompression = $decompression -bor [System.Net.DecompressionMethods]::Brotli
    }
    $handler.AutomaticDecompression = $decompression

    $client = [System.Net.Http.HttpClient]::new($handler, $true)
    $client.Timeout = [timespan]::FromSeconds($RequestTimeoutSeconds)
    $client.DefaultRequestHeaders.UserAgent.ParseAdd($UserAgent)
    $client.DefaultRequestHeaders.Accept.ParseAdd('text/html,application/xhtml+xml;q=0.9,*/*;q=0.5')
    $client.DefaultRequestHeaders.AcceptLanguage.ParseAdd('en-US,en;q=0.9')

    $script:HttpHandler = $handler
    $script:HttpClient = $client
}

function New-GoodreadsRequestMessage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [uri]$Uri
    )

    if (-not (Test-AllowedGoodreadsUri -Uri $Uri)) {
        throw "Refusing request to disallowed URI '$Uri'."
    }

    $request = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::Get, $Uri)
    $request.Headers.Referrer = [uri]'https://www.goodreads.com/'
    return $request
}

function Invoke-GoodreadsTextRequest {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [uri]$Uri,

        [Parameter(Mandatory)]
        [string]$Purpose
    )

    $currentUri = $Uri
    $attempt = 0
    $redirects = 0

    while ($true) {
        Wait-ForRequestSlot
        $request = New-GoodreadsRequestMessage -Uri $currentUri
        $response = $null

        try {
            Write-RunLog -Level DEBUG -Message "GET $Purpose ($currentUri)"
            $response = $script:HttpClient.SendAsync(
                $request,
                [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead
            ).GetAwaiter().GetResult()

            $status = [int]$response.StatusCode
            if ($status -in @(301, 302, 303, 307, 308)) {
                if ($redirects -ge 5 -or $null -eq $response.Headers.Location) {
                    throw "Too many or invalid redirects while requesting $Purpose."
                }

                $currentUri = Resolve-GoodreadsRedirect -CurrentUri $currentUri -Location $response.Headers.Location
                $redirects++
                continue
            }

            $challenge = Test-GoodreadsChallengeResponse -Response $response
            if ($status -eq 200 -and -not $challenge) {
                $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
                if ([string]::IsNullOrWhiteSpace($body)) {
                    throw "Goodreads returned an empty response for $Purpose."
                }
                Register-GoodreadsSuccess
                return $body
            }

            $transient = $challenge -or $status -in @(202, 408, 425, 429, 500, 502, 503, 504)
            if (-not $transient -or $attempt -ge $MaxRetries) {
                throw "Goodreads returned HTTP $status for $Purpose."
            }

            if ($challenge -or $status -eq 429) {
                Register-GoodreadsThrottle
            }
            $attempt++
            $delay = Get-RetryDelay -Attempt $attempt -Response $response
            Write-RunLog -Level WARN -Message "HTTP $status for $Purpose; retry $attempt/$MaxRetries in $delay ms."
            Start-Sleep -Milliseconds $delay
        }
        catch {
            if ($attempt -ge $MaxRetries -or $_.Exception.Message -match '^Goodreads returned HTTP (?!408|425|429|500|502|503|504)') {
                throw
            }

            $attempt++
            $delay = Get-RetryDelay -Attempt $attempt -Response $response
            Write-RunLog -Level WARN -Message "Request failed for ${Purpose}: $($_.Exception.Message); retry $attempt/$MaxRetries in $delay ms."
            Start-Sleep -Milliseconds $delay
        }
        finally {
            if ($null -ne $response) {
                $response.Dispose()
            }
            $request.Dispose()
        }
    }
}

function Read-ResponsePrefix {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [System.Net.Http.HttpResponseMessage]$Response,

        [ValidateRange(32768, 1048576)]
        [int]$MaximumCharacters = 262144
    )

    $stream = $Response.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
    $reader = [System.IO.StreamReader]::new($stream, [System.Text.Encoding]::UTF8, $true, 8192, $false)
    $builder = [System.Text.StringBuilder]::new([math]::Min($MaximumCharacters, 65536))
    $buffer = [char[]]::new(8192)

    try {
        while ($builder.Length -lt $MaximumCharacters) {
            $remaining = $MaximumCharacters - $builder.Length
            $toRead = [math]::Min($buffer.Length, $remaining)
            $count = $reader.Read($buffer, 0, $toRead)
            if ($count -le 0) {
                break
            }

            [void]$builder.Append($buffer, 0, $count)
            $current = $builder.ToString()
            if ($current.Contains('application/ld+json', [System.StringComparison]::OrdinalIgnoreCase) -and
                $current.Contains('data-testid="bookTitle"', [System.StringComparison]::OrdinalIgnoreCase) -and
                $current.Contains('</h1>', [System.StringComparison]::OrdinalIgnoreCase)) {
                break
            }
        }

        return $builder.ToString()
    }
    finally {
        $reader.Dispose()
    }
}

function ConvertFrom-BookDetailHtml {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Html,

        [Parameter(Mandatory)]
        [string]$BookId
    )

    $jsonLdMatch = [regex]::Match(
        $Html,
        '(?is)<script\b(?=[^>]*\btype=["'']application/ld\+json["''])[^>]*>(?<json>.*?)</script>'
    )
    if (-not $jsonLdMatch.Success) {
        throw "No Book JSON-LD metadata was found."
    }

    try {
        $schema = $jsonLdMatch.Groups['json'].Value | ConvertFrom-Json -Depth 32
    }
    catch {
        throw "Book JSON-LD was invalid: $($_.Exception.Message)"
    }

    if ($schema -is [array]) {
        $schema = @($schema | Where-Object { (Get-ObjectPropertyValue -InputObject $_ -Name '@type') -eq 'Book' }) |
            Select-Object -First 1
    }
    if ($null -eq $schema -or (Get-ObjectPropertyValue -InputObject $schema -Name '@type') -ne 'Book') {
        throw "JSON-LD did not contain a Book object."
    }

    $pages = 0
    $pageValue = Get-ObjectPropertyValue -InputObject $schema -Name 'numberOfPages'
    if ($null -ne $pageValue) {
        [void][int]::TryParse([string]$pageValue, [ref]$pages)
    }
    if ($pages -lt 1 -or $pages -gt 100000) {
        $pages = 0
    }

    $authorIds = [System.Collections.Generic.List[string]]::new()
    $schemaAuthors = Get-ObjectPropertyValue -InputObject $schema -Name 'author'
    foreach ($authorNode in @($schemaAuthors)) {
        $authorUrl = Get-ObjectPropertyValue -InputObject $authorNode -Name 'url'
        if ($null -eq $authorNode -or [string]::IsNullOrWhiteSpace([string]$authorUrl)) {
            continue
        }

        $authorMatch = [regex]::Match([string]$authorUrl, '/author/show/(?<id>\d+)', 'IgnoreCase')
        if ($authorMatch.Success -and -not $authorIds.Contains($authorMatch.Groups['id'].Value)) {
            [void]$authorIds.Add($authorMatch.Groups['id'].Value)
        }
    }

    $seriesId = ''
    $seriesName = ''
    $seriesNumber = ''
    $seriesMatch = [regex]::Match(
        $Html,
        '(?is)<h3\b(?=[^>]*\baria-label=["'']Book\s+(?<number>.+?)\s+in\s+the\s+(?<series>.+?)\s+series["''])[^>]*>.*?<a\b[^>]*\bhref=["''][^"'']*/series/(?<id>\d+)[^"'']*["''][^>]*>',
        ([System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Singleline)
    )
    if ($seriesMatch.Success) {
        $seriesId = $seriesMatch.Groups['id'].Value
        $seriesName = ConvertFrom-HtmlText $seriesMatch.Groups['series'].Value
        $seriesNumber = ConvertFrom-HtmlText $seriesMatch.Groups['number'].Value
    }

    $displayTitle = ''
    $titleMatch = [regex]::Match(
        $Html,
        '(?is)<h1\b(?=[^>]*\bdata-testid=["'']bookTitle["''])[^>]*>(?<title>.*?)</h1>'
    )
    if ($titleMatch.Success) {
        $displayTitle = ConvertFrom-HtmlText $titleMatch.Groups['title'].Value
    }

    return [pscustomobject]@{
        BookId       = $BookId
        Pages        = $pages
        Format       = [string](Get-ObjectPropertyValue -InputObject $schema -Name 'bookFormat')
        Language     = [string](Get-ObjectPropertyValue -InputObject $schema -Name 'inLanguage')
        DetailTitle  = $displayTitle
        AuthorIds    = @($authorIds)
        SeriesId     = $seriesId
        SeriesName   = $seriesName
        SeriesNumber = $seriesNumber
    }
}

function Get-BookDetailMap {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Books
    )

    $results = @{}
    $pending = [System.Collections.Generic.List[object]]::new()

    foreach ($book in $Books) {
        $bookId = [string]$book.BookId
        [void]$pending.Add([pscustomobject]@{
            BookId       = $bookId
            Title        = [string]$book.Title
            Uri          = [uri]("https://www.goodreads.com/book/show/$bookId")
            Attempt      = 0
            Redirects    = 0
        })
    }

    Write-RunLog -Level INFO -Message "Book details: $($pending.Count) page request(s) required."

    while ($pending.Count -gt 0) {
        $retryQueue = [System.Collections.Generic.List[object]]::new()
        $largestRetryDelay = 0

        for ($offset = 0; $offset -lt $pending.Count; $offset += $MaxConcurrency) {
            $operationCount = [math]::Min($MaxConcurrency, $pending.Count - $offset)
            $operations = [System.Collections.Generic.List[object]]::new()

            for ($index = 0; $index -lt $operationCount; $index++) {
                Wait-ForRequestSlot
                $state = $pending[$offset + $index]
                $request = New-GoodreadsRequestMessage -Uri $state.Uri
                try {
                    $task = $script:HttpClient.SendAsync(
                        $request,
                        [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead
                    )
                    [void]$operations.Add([pscustomobject]@{
                        State   = $state
                        Request = $request
                        Task    = $task
                    })
                }
                catch {
                    $request.Dispose()
                    $state.Attempt++
                    if ($state.Attempt -le $MaxRetries) {
                        [void]$retryQueue.Add($state)
                        $largestRetryDelay = [math]::Max(
                            $largestRetryDelay,
                            (Get-RetryDelay -Attempt $state.Attempt -Response $null)
                        )
                    }
                    else {
                        Write-RunLog -Level ERROR -Message "Book-detail request failed for '$($state.Title)' after retries: $($_.Exception.Message)"
                    }
                }
            }

            foreach ($operation in $operations) {
                $state = $operation.State
                $response = $null
                try {
                    $response = $operation.Task.GetAwaiter().GetResult()
                    $status = [int]$response.StatusCode

                    if ($status -in @(301, 302, 303, 307, 308)) {
                        if ($state.Redirects -ge 5 -or $null -eq $response.Headers.Location) {
                            throw "Too many or invalid redirects."
                        }

                        $state.Uri = Resolve-GoodreadsRedirect -CurrentUri $state.Uri -Location $response.Headers.Location
                        $state.Redirects++
                        [void]$retryQueue.Add($state)
                        continue
                    }

                    $challenge = Test-GoodreadsChallengeResponse -Response $response
                    if ($status -ne 200 -or $challenge) {
                        $transient = $challenge -or $status -in @(202, 408, 425, 429, 500, 502, 503, 504)
                        if ($transient -and $state.Attempt -lt $MaxRetries) {
                            if ($challenge -or $status -eq 429) {
                                Register-GoodreadsThrottle
                            }
                            $state.Attempt++
                            [void]$retryQueue.Add($state)
                            $largestRetryDelay = [math]::Max(
                                $largestRetryDelay,
                                (Get-RetryDelay -Attempt $state.Attempt -Response $response)
                            )
                            Write-RunLog -Level WARN -Message "HTTP $status for '$($state.Title)'; queued retry $($state.Attempt)/$MaxRetries."
                            continue
                        }

                        throw "Goodreads returned HTTP $status."
                    }

                    $prefix = Read-ResponsePrefix -Response $response
                    try {
                        $metadata = ConvertFrom-BookDetailHtml -Html $prefix -BookId $state.BookId
                    }
                    catch {
                        if ($state.Attempt -lt $MaxRetries) {
                            $state.Attempt++
                            [void]$retryQueue.Add($state)
                            $largestRetryDelay = [math]::Max(
                                $largestRetryDelay,
                                (Get-RetryDelay -Attempt $state.Attempt -Response $null)
                            )
                            Write-RunLog -Level WARN -Message "Could not parse '$($state.Title)': $($_.Exception.Message); queued retry $($state.Attempt)/$MaxRetries."
                            continue
                        }
                        throw
                    }

                    $results[$state.BookId] = $metadata
                    Register-GoodreadsSuccess
                    Write-RunLog -Level DEBUG -Message "Resolved detail metadata for '$($state.Title)' ($($metadata.Pages) pages)."
                }
                catch {
                    if ($state.Attempt -lt $MaxRetries -and
                        $_.Exception.Message -notmatch '^Goodreads returned HTTP (?!408|425|429|500|502|503|504)') {
                        $state.Attempt++
                        [void]$retryQueue.Add($state)
                        $largestRetryDelay = [math]::Max(
                            $largestRetryDelay,
                            (Get-RetryDelay -Attempt $state.Attempt -Response $response)
                        )
                        Write-RunLog -Level WARN -Message "Book-detail request failed for '$($state.Title)': $($_.Exception.Message); queued retry $($state.Attempt)/$MaxRetries."
                    }
                    else {
                        Write-RunLog -Level ERROR -Message "No usable detail metadata for '$($state.Title)': $($_.Exception.Message)"
                    }
                }
                finally {
                    if ($null -ne $response) {
                        $response.Dispose()
                    }
                    $operation.Request.Dispose()
                }
            }

        }

        $pending = $retryQueue
        if ($pending.Count -gt 0 -and $largestRetryDelay -gt 0) {
            Write-RunLog -Level WARN -Message "Waiting $largestRetryDelay ms before retrying $($pending.Count) book-detail request(s)."
            Start-Sleep -Milliseconds ([int][math]::Min(30000, $largestRetryDelay))
        }
    }

    return $results
}

function Get-TitleExclusionReason {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Title
    )

    if ($Title -match '(?i)\b(?:dramati[sz]ed\s+adaptation|audio\s*book|audiobook|audio\s+adaptation|full[- ]cast\s+audio)\b') {
        return 'Audio or dramatized adaptation'
    }

    if ($Title -match '(?i)\b(?:box(?:ed)?\s*set|boxset|omnibus|bind[- ]?up|bundle|series\s+collection|collection\s+set|books?\s+collection|complete\s+(?:book\s+)?series|books?\s+(?:set|bundle)|collection\s+#?\d+\s*[-–—]\s*\d+)\b' -or
        $Title -match '(?i)\bbooks?\s+#?\d+\s*[-–—]\s*\d+\b' -or
        $Title -match '(?i)#\d+\s*[-–—]\s*\d+\b') {
        return 'Omnibus, box set, or multi-work collection'
    }

    return ''
}

function Split-TitleAndSeries {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$CompleteTitle
    )

    $match = [regex]::Match(
        $CompleteTitle,
        '^(?<title>.+?)\s+\((?<series>.+?)(?:,\s*|\s+)#(?<number>[^()]+)\)\s*$'
    )

    if ($match.Success) {
        return [pscustomobject]@{
            Title        = $match.Groups['title'].Value.Trim()
            SeriesName   = $match.Groups['series'].Value.Trim()
            SeriesNumber = $match.Groups['number'].Value.Trim()
        }
    }

    return [pscustomobject]@{
        Title        = $CompleteTitle.Trim()
        SeriesName   = ''
        SeriesNumber = ''
    }
}

function Get-NormalizedSeriesKey {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$SeriesName
    )

    if ([string]::IsNullOrWhiteSpace($SeriesName)) {
        return ''
    }

    $normalized = $SeriesName.Normalize([System.Text.NormalizationForm]::FormKC)
    $normalized = $normalized.Replace('&', ' AND ').ToUpperInvariant()
    return ([regex]::Replace($normalized, '[^\p{L}\p{Nd}]+', ' ')).Trim()
}

function Get-NormalizedTitleKey {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Title
    )

    if ([string]::IsNullOrWhiteSpace($Title)) {
        return ''
    }

    $normalized = $Title.Normalize([System.Text.NormalizationForm]::FormKC)
    return ([regex]::Replace($normalized, '\s+', ' ')).Trim().ToUpperInvariant()
}

function Get-UniqueTitleBookList {
    [CmdletBinding()]
    [OutputType([object[]])]
    param(
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [object[]]$Books,

        [Parameter(Mandatory)]
        [string]$AuthorName
    )

    $uniqueBooks = [System.Collections.Generic.List[object]]::new()
    foreach ($group in ($Books | Group-Object -Property {
        Get-NormalizedTitleKey -Title ([string]$_.Title)
    })) {
        $ordered = @($group.Group | Sort-Object -Property (
            @{ Expression = 'RatingCount'; Descending = $true },
            @{ Expression = { [string]::IsNullOrWhiteSpace([string]$_.SeriesName) }; Ascending = $true },
            @{ Expression = { [long]$_.BookId }; Ascending = $true }
        ))
        $selected = $ordered[0]

        # If the highest-count copy omits series metadata, safely retain it from
        # duplicates only when every series-bearing copy agrees on the series.
        $seriesDonors = @($ordered | Where-Object {
            -not [string]::IsNullOrWhiteSpace([string]$_.SeriesName)
        })
        if ([string]::IsNullOrWhiteSpace([string]$selected.SeriesName) -and
            $seriesDonors.Count -gt 0) {
            $donorKeys = @($seriesDonors | ForEach-Object {
                Get-NormalizedSeriesKey -SeriesName ([string]$_.SeriesName)
            } | Select-Object -Unique)
            if ($donorKeys.Count -eq 1) {
                $selected.SeriesId = [string]$seriesDonors[0].SeriesId
                $selected.SeriesName = [string]$seriesDonors[0].SeriesName
                $selected.SeriesNumber = [string]$seriesDonors[0].SeriesNumber
            }
        }
        elseif ([string]::IsNullOrWhiteSpace([string]$selected.SeriesNumber)) {
            $selectedSeriesKey = Get-NormalizedSeriesKey -SeriesName ([string]$selected.SeriesName)
            $numberDonor = @($seriesDonors | Where-Object {
                (Get-NormalizedSeriesKey -SeriesName ([string]$_.SeriesName)) -eq $selectedSeriesKey -and
                -not [string]::IsNullOrWhiteSpace([string]$_.SeriesNumber)
            }) | Select-Object -First 1
            if ($null -ne $numberDonor) {
                $selected.SeriesNumber = [string]$numberDonor.SeriesNumber
            }
        }

        [void]$uniqueBooks.Add($selected)
        for ($index = 1; $index -lt $ordered.Count; $index++) {
            Add-Exclusion -Reason 'Duplicate title' -Author $AuthorName -Title $ordered[$index].Title
        }

        if ($ordered.Count -gt 1) {
            Write-RunLog -Level DEBUG -Message (
                "Duplicate title '$($selected.Title)' for '$AuthorName': kept book $($selected.BookId) with $($selected.RatingCount) ratings; discarded $($ordered.Count - 1) lower-priority copy/copies."
            )
        }
    }

    return @($uniqueBooks)
}

function Test-PrimaryAuthorRole {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Role
    )

    if ([string]::IsNullOrWhiteSpace($Role)) {
        return $true
    }

    return $Role.Trim() -match '^(?i:Goodreads Author|Author|Co-Author|Writer|Creator|Story|Primary Contributor|Main Author)$'
}

function ConvertFrom-AuthorListPage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Html,

        [Parameter(Mandatory)]
        [string]$InputAuthorId
    )

    $authorName = ''
    $headingMatch = [regex]::Match($Html, '(?is)<h1\b[^>]*>\s*Books\s+by\s+(?<name>.*?)</h1>')
    if ($headingMatch.Success) {
        $authorName = ConvertFrom-HtmlText $headingMatch.Groups['name'].Value
    }

    $rows = [System.Collections.Generic.List[object]]::new()
    $pageMinimumRatingsCount = [long]::MaxValue
    $rowMatches = [regex]::Matches(
        $Html,
        '(?is)<tr\b[^>]*\bitemtype=["'']http://schema\.org/Book["''][^>]*>(?<row>.*?)</tr>'
    )

    foreach ($rowMatch in $rowMatches) {
        $row = $rowMatch.Groups['row'].Value
        $rowText = ConvertFrom-HtmlText $row
        $ratingMatch = [regex]::Match(
            $rowText,
            '(?<rating>\d+(?:\.\d+)?)\s+avg rating\s+(?:\p{Pd}|-)+\s+(?<count>[\d,]+)\s+ratings?',
            'IgnoreCase'
        )
        $rating = 0.0
        $ratingCount = [long]0
        $ratingMetadataValid = $false
        if ($ratingMatch.Success) {
            try {
                $rating = ConvertTo-InvariantNumber -Value $ratingMatch.Groups['rating'].Value -Type Double
                $ratingCount = ConvertTo-InvariantNumber -Value $ratingMatch.Groups['count'].Value -Type Int64
                $ratingMetadataValid = $true
                $pageMinimumRatingsCount = [math]::Min($pageMinimumRatingsCount, $ratingCount)
            }
            catch {
                $ratingMetadataValid = $false
            }
        }

        $titleMatch = [regex]::Match(
            $row,
            '(?is)<a\b(?=[^>]*\bclass=["''][^"'']*\bbookTitle\b[^"'']*["''])(?=[^>]*\bhref=["''](?<href>[^"'']+)["''])[^>]*>.*?<span\b[^>]*\bitemprop=["'']name["''][^>]*>(?<title>.*?)</span>'
        )
        if (-not $titleMatch.Success) {
            Add-Exclusion -Reason 'Unparseable author-list row' -Author $authorName
            continue
        }

        $href = [System.Net.WebUtility]::HtmlDecode($titleMatch.Groups['href'].Value)
        $bookMatch = [regex]::Match($href, '/book/show/(?<id>\d+)', 'IgnoreCase')
        if (-not $bookMatch.Success) {
            Add-Exclusion -Reason 'Missing Goodreads book ID' -Author $authorName
            continue
        }

        $completeTitle = ConvertFrom-HtmlText $titleMatch.Groups['title'].Value
        $titleParts = Split-TitleAndSeries -CompleteTitle $completeTitle
        $titleReason = Get-TitleExclusionReason -Title $completeTitle
        if (-not [string]::IsNullOrWhiteSpace($titleReason)) {
            Add-Exclusion -Reason $titleReason -Author $authorName -Title $completeTitle
            continue
        }

        $authorMatches = [regex]::Matches(
            $row,
            '(?is)<a\b(?=[^>]*\bclass=["''][^"'']*\bauthorName\b[^"'']*["''])(?=[^>]*\bhref=["''][^"'']*/author/show/(?<id>\d+)[^"'']*["''])[^>]*>.*?<span\b[^>]*\bitemprop=["'']name["''][^>]*>(?<name>.*?)</span>.*?</a>(?<after>.{0,200})'
        )

        $inputAuthorFound = $false
        $inputAuthorIsPrimary = $false
        foreach ($authorMatch in $authorMatches) {
            if ($authorMatch.Groups['id'].Value -ne $InputAuthorId) {
                continue
            }

            $inputAuthorFound = $true
            $role = ''
            $roleMatch = [regex]::Match(
                $authorMatch.Groups['after'].Value,
                '(?is)<span\b(?=[^>]*\bclass=["''][^"'']*\brole\b[^"'']*["''])[^>]*>\s*\((?<role>[^)]+)\)\s*</span>',
                'IgnoreCase'
            )
            if (-not $roleMatch.Success) {
                $roleMatch = [regex]::Match(
                    $authorMatch.Groups['after'].Value,
                    '^\s*<span\b[^>]*>\s*\((?<role>[^)]+)\)\s*</span>',
                    'IgnoreCase'
                )
            }
            if ($roleMatch.Success) {
                $role = ConvertFrom-HtmlText $roleMatch.Groups['role'].Value
            }

            if (Test-PrimaryAuthorRole -Role $role) {
                $inputAuthorIsPrimary = $true
            }
            break
        }

        if (-not $inputAuthorFound -or -not $inputAuthorIsPrimary) {
            Add-Exclusion -Reason 'Contributor-only credit' -Author $authorName -Title $completeTitle
            continue
        }

        if (-not $ratingMatch.Success) {
            Add-Exclusion -Reason 'Missing rating metadata' -Author $authorName -Title $completeTitle
            continue
        }
        if (-not $ratingMetadataValid) {
            Add-Exclusion -Reason 'Invalid rating metadata' -Author $authorName -Title $completeTitle
            continue
        }

        $publicationYear = 0
        $yearMatch = [regex]::Match($rowText, '\bpublished\s+(?<year>\d{4})\b', 'IgnoreCase')
        if (-not $yearMatch.Success -or
            -not [int]::TryParse($yearMatch.Groups['year'].Value, [ref]$publicationYear)) {
            Add-Exclusion -Reason 'Missing publication year' -Author $authorName -Title $completeTitle
            continue
        }

        $workId = ''
        $workMatch = [regex]::Match($row, '/work/editions/(?<id>\d+)', 'IgnoreCase')
        if ($workMatch.Success) {
            $workId = $workMatch.Groups['id'].Value
        }

        [void]$rows.Add([pscustomobject]@{
            AuthorId       = $InputAuthorId
            Author         = $authorName
            BookId         = $bookMatch.Groups['id'].Value
            WorkId         = $workId
            WorkKey        = if ($workId) { "work:$workId" } else { "book:$($bookMatch.Groups['id'].Value)" }
            Title          = $titleParts.Title
            CompleteTitle  = $completeTitle
            SeriesId       = ''
            SeriesName     = $titleParts.SeriesName
            SeriesNumber   = $titleParts.SeriesNumber
            AverageRating  = [double]$rating
            RatingCount    = [long]$ratingCount
            PublicationYear = $publicationYear
        })
    }

    $nextHref = ''
    $nextMatch = [regex]::Match(
        $Html,
        '(?is)<a\b(?=[^>]*\bclass=["''][^"'']*\bnext_page\b[^"'']*["''])(?=[^>]*\brel=["'']next["''])(?=[^>]*\bhref=["''](?<href>[^"'']+)["''])[^>]*>'
    )
    if ($nextMatch.Success) {
        $nextHref = [System.Net.WebUtility]::HtmlDecode($nextMatch.Groups['href'].Value)
    }

    return [pscustomobject]@{
        AuthorName                  = $authorName
        Books                       = @($rows)
        NextHref                    = $nextHref
        HasBookBelowRatingsThreshold = $pageMinimumRatingsCount -lt [long]$MinimumRatingsCount
        MinimumPageRatingsCount     = if ($pageMinimumRatingsCount -eq [long]::MaxValue) {
            $null
        }
        else {
            $pageMinimumRatingsCount
        }
    }
}

function Get-GoodreadsAuthorRecord {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [psobject]$Config
    )

    $allBooks = [System.Collections.Generic.List[object]]::new()
    $visited = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $currentUri = [uri]$Config.Url
    $authorName = ''
    $pageNumber = 1
    $forceInclude = [bool](Get-ObjectPropertyValue -InputObject $Config -Name 'ForceInclude')
    $paginationCutoffBypassed = $false

    while ($true) {
        if (-not $visited.Add($currentUri.AbsoluteUri)) {
            throw "Pagination loop detected for author ID $($Config.AuthorId)."
        }

        $html = Invoke-GoodreadsTextRequest -Uri $currentUri -Purpose (
            "author $($Config.AuthorId), page $pageNumber"
        )

        $parsed = ConvertFrom-AuthorListPage -Html $html -InputAuthorId $Config.AuthorId
        if (-not [string]::IsNullOrWhiteSpace($parsed.AuthorName)) {
            $authorName = $parsed.AuthorName
        }
        foreach ($book in $parsed.Books) {
            $book | Add-Member -NotePropertyName Genre -NotePropertyValue $Config.Genre
            [void]$allBooks.Add($book)
        }

        Write-RunLog -Level INFO -Message (
            "Parsed author $($Config.AuthorId), page ${pageNumber}: $($parsed.Books.Count) eligible row(s)."
        )

        if ([string]::IsNullOrWhiteSpace($parsed.NextHref)) {
            break
        }

        if ($parsed.HasBookBelowRatingsThreshold -and -not $forceInclude) {
            Write-RunLog -Level INFO -Message (
                "Stopped pagination for author $($Config.AuthorId) after page ${pageNumber}: lowest listed ratings count $($parsed.MinimumPageRatingsCount) is below threshold $MinimumRatingsCount."
            )
            break
        }

        if ($parsed.HasBookBelowRatingsThreshold -and
            $forceInclude -and
            -not $paginationCutoffBypassed) {
            Write-RunLog -Level INFO -Message (
                "Continuing pagination for force-included author $($Config.AuthorId) despite a listed ratings count below $MinimumRatingsCount."
            )
            $paginationCutoffBypassed = $true
        }

        $nextUri = [uri]::new($currentUri, $parsed.NextHref)
        if (-not (Test-AllowedGoodreadsUri -Uri $nextUri) -or
            $nextUri.AbsolutePath -notmatch "^/author/list/$([regex]::Escape($Config.AuthorId))(?:\.|/|$)") {
            throw "Unsafe or unexpected pagination link '$nextUri'."
        }

        $currentUri = $nextUri
        $pageNumber++
    }

    if ([string]::IsNullOrWhiteSpace($authorName)) {
        $authorName = "Goodreads Author $($Config.AuthorId)"
    }

    # Goodreads advertises distinct works, but defensive work-ID and title
    # deduplication protects against pagination drift and duplicate editions.
    $deduplicated = [System.Collections.Generic.List[object]]::new()
    foreach ($group in ($allBooks | Group-Object -Property WorkKey)) {
        $selected = @($group.Group | Sort-Object -Property (
            @{ Expression = 'RatingCount'; Descending = $true },
            @{ Expression = 'BookId'; Ascending = $true }
        ))[0]
        $selected.Author = $authorName
        [void]$deduplicated.Add($selected)

        if ($group.Count -gt 1) {
            Add-Exclusion -Reason 'Duplicate Goodreads work' -Author $authorName -Title $selected.Title
        }
    }

    $titleDeduplicated = @(Get-UniqueTitleBookList -Books @($deduplicated) -AuthorName $authorName)

    return [pscustomobject]@{
        AuthorId = $Config.AuthorId
        Author   = $authorName
        Genre    = $Config.Genre
        ForceInclude = $forceInclude
        Books    = $titleDeduplicated
    }
}

function Import-AuthorConfiguration {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$LiteralPath
    )

    $firstNonBlank = Get-Content -LiteralPath $LiteralPath -TotalCount 20 |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Select-Object -First 1
    if ([string]::IsNullOrWhiteSpace($firstNonBlank)) {
        throw "Input file '$LiteralPath' is empty."
    }

    $delimiter = if ($firstNonBlank.Contains("`t")) { "`t" } else { ',' }
    $records = @(Import-Csv -LiteralPath $LiteralPath -Delimiter $delimiter)
    if ($records.Count -eq 0) {
        throw "Input file '$LiteralPath' contains no data rows."
    }

    $headers = @($records[0].PSObject.Properties.Name)
    $urlHeader = @($headers | Where-Object { $_.Trim() -ieq 'URL' }) | Select-Object -First 1
    $genreHeader = @($headers | Where-Object { $_.Trim() -ieq 'Genre' }) | Select-Object -First 1
    $authorHeader = @($headers | Where-Object { $_.Trim() -ieq 'Author' }) | Select-Object -First 1
    $excludeHeader = @($headers | Where-Object { $_.Trim() -ieq 'Exclude' }) | Select-Object -First 1
    $includeHeader = @($headers | Where-Object { $_.Trim() -ieq 'Include' }) | Select-Object -First 1
    if ($null -eq $urlHeader -or $null -eq $genreHeader) {
        throw "Input must contain URL and Genre columns. Found: $($headers -join ', ')."
    }

    $configs = [System.Collections.Generic.List[object]]::new()
    $seenGenres = @{}
    $seenForceInclude = @{}
    $excludedRowCount = 0
    $forceIncludedRowCount = 0
    $rowNumber = 1
    foreach ($record in $records) {
        $rowNumber++
        $rawUrl = [string]$record.$urlHeader
        $genre = ([regex]::Replace([string]$record.$genreHeader, '\s+', ' ')).Trim()
        $excludeValue = if ($null -ne $excludeHeader) {
            ([string]$record.$excludeHeader).Trim()
        }
        else {
            ''
        }
        $includeValue = if ($null -ne $includeHeader) {
            ([string]$record.$includeHeader).Trim()
        }
        else {
            ''
        }
        $excludeRequested = $excludeValue -ieq 'Y'
        $forceInclude = $includeValue -ieq 'Y'

        if ($excludeRequested -and $forceInclude) {
            throw "Input row $rowNumber cannot have Y in both Exclude and Include."
        }

        if ($excludeRequested) {
            $excludedRowCount++
            $authorLabel = if ($null -ne $authorHeader) {
                ([regex]::Replace([string]$record.$authorHeader, '\s+', ' ')).Trim()
            }
            else {
                ''
            }
            if ([string]::IsNullOrWhiteSpace($authorLabel)) {
                $authorLabel = $rawUrl.Trim()
            }
            if ([string]::IsNullOrWhiteSpace($authorLabel)) {
                $authorLabel = "input row $rowNumber"
            }

            Write-RunLog -Level INFO -Message (
                "Excluded '$authorLabel' from input row $rowNumber because Exclude=Y; no Goodreads lookup will be performed."
            )
            continue
        }

        if ([string]::IsNullOrWhiteSpace($rawUrl) -and [string]::IsNullOrWhiteSpace($genre)) {
            continue
        }
        if ([string]::IsNullOrWhiteSpace($rawUrl) -or [string]::IsNullOrWhiteSpace($genre)) {
            throw "Input row $rowNumber must contain both URL and Genre."
        }

        $normalized = ConvertTo-NormalizedAuthorUrl -Value $rawUrl
        if ($seenGenres.ContainsKey($normalized.AuthorId)) {
            if ($seenGenres[$normalized.AuthorId] -ine $genre) {
                throw "Author ID $($normalized.AuthorId) appears with conflicting genres '$($seenGenres[$normalized.AuthorId])' and '$genre'."
            }
            if ([bool]$seenForceInclude[$normalized.AuthorId] -ne $forceInclude) {
                throw "Author ID $($normalized.AuthorId) appears with conflicting Include values."
            }
            Write-RunLog -Level WARN -Message "Ignoring duplicate input row for author ID $($normalized.AuthorId)."
            continue
        }

        $seenGenres[$normalized.AuthorId] = $genre
        $seenForceInclude[$normalized.AuthorId] = $forceInclude
        [void]$configs.Add([pscustomobject]@{
            AuthorId    = $normalized.AuthorId
            Url         = $normalized.Url
            Genre       = $genre
            ForceInclude = $forceInclude
        })

        if ($forceInclude) {
            $forceIncludedRowCount++
            $authorLabel = if ($null -ne $authorHeader) {
                ([regex]::Replace([string]$record.$authorHeader, '\s+', ' ')).Trim()
            }
            else {
                ''
            }
            if ([string]::IsNullOrWhiteSpace($authorLabel)) {
                $authorLabel = $normalized.Url
            }
            Write-RunLog -Level INFO -Message (
                "Force-including '$authorLabel' from input row $rowNumber because Include=Y; ratings-based pagination and filtering will be bypassed."
            )
        }
    }

    if ($excludedRowCount -gt 0) {
        Write-RunLog -Level INFO -Message "Skipped $excludedRowCount input author row(s) marked Exclude=Y."
    }
    if ($forceIncludedRowCount -gt 0) {
        Write-RunLog -Level INFO -Message "Enabled force-inclusion for $forceIncludedRowCount input author row(s) marked Include=Y."
    }

    if ($configs.Count -eq 0) {
        throw 'No valid author rows were found in the input.'
    }

    return @($configs)
}

function Test-UsableBookDetail {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [psobject]$Book,

        [Parameter(Mandatory)]
        [psobject]$Detail
    )

    if ([int]$Detail.Pages -le 0) {
        Add-Exclusion -Reason 'Missing usable page count' -Author $Book.Author -Title $Book.Title
        return $false
    }

    if ([string]$Detail.Format -match '(?i)\b(?:audio|audible|mp3|cassette|podcast)\b') {
        Add-Exclusion -Reason 'Audio edition format' -Author $Book.Author -Title $Book.Title
        return $false
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$Detail.Language) -and
        [string]$Detail.Language -notmatch '^(?i:English|en(?:[-_].*)?)$') {
        Add-Exclusion -Reason 'Non-English translated edition' -Author $Book.Author -Title $Book.Title
        return $false
    }

    $detailAuthorIds = @($Detail.AuthorIds | ForEach-Object { [string]$_ })
    if ($detailAuthorIds.Count -gt 0 -and $Book.AuthorId -notin $detailAuthorIds) {
        Add-Exclusion -Reason 'Input author absent from canonical edition' -Author $Book.Author -Title $Book.Title
        return $false
    }

    return $true
}

function Get-SeriesNumberSortKey {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$SeriesNumber
    )

    if ([string]::IsNullOrWhiteSpace($SeriesNumber)) {
        return '9999999999|'
    }

    $match = [regex]::Match($SeriesNumber.Trim(), '^(?<number>\d+(?:\.\d+)?)')
    if ($match.Success) {
        $number = ConvertTo-InvariantNumber -Value $match.Groups['number'].Value -Type Double
        return '{0:D10}|{1}' -f [int64][math]::Round($number * 1000), $SeriesNumber.ToUpperInvariant()
    }

    return '9999999998|' + $SeriesNumber.ToUpperInvariant()
}

function Get-SeriesFirstBook {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Books
    )

    $numberOne = @($Books | Where-Object {
        $numeric = 0.0
        [double]::TryParse(
            ([string]$_.SeriesNumber).Trim(),
            [System.Globalization.NumberStyles]::Float,
            $script:InvariantCulture,
            [ref]$numeric
        ) -and [math]::Abs($numeric - 1.0) -lt 0.0000001
    } | Sort-Object -Property (
        @{ Expression = 'PublicationYear'; Ascending = $true },
        @{ Expression = 'Title'; Ascending = $true }
    ))

    if ($numberOne.Count -gt 0) {
        return $numberOne[0]
    }

    return @($Books | Sort-Object -Property (
        @{ Expression = { Get-SeriesNumberSortKey -SeriesNumber $_.SeriesNumber }; Ascending = $true },
        @{ Expression = 'PublicationYear'; Ascending = $true },
        @{ Expression = 'Title'; Ascending = $true }
    ))[0]
}

function Get-UnweightedAverage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Values
    )

    if ($Values.Count -eq 0) {
        throw 'Cannot calculate an average of zero values.'
    }

    $sum = 0.0
    foreach ($value in $Values) {
        $sum += [double]$value
    }
    return $sum / $Values.Count
}

function Test-PossibleSeriesTitle {
    [CmdletBinding()]
    [OutputType([bool])]
    param(
        [Parameter(Mandatory)]
        [string]$Title
    )

    return $Title -match '(?i)(?:#\s*\d+(?:\.\d+)?|\b(?:book|vol(?:ume)?|part)\s*[#:]?\s*\d+(?:\.\d+)?\b|\b(?:series|trilogy|saga|cycle)\b)'
}

function Get-DetailCandidateBookList {
    [CmdletBinding()]
    [OutputType([object[]])]
    param(
        [Parameter(Mandatory)]
        [object[]]$AuthorRecords
    )

    $candidates = [System.Collections.Generic.List[object]]::new()

    foreach ($authorRecord in $AuthorRecords) {
        foreach ($book in $authorRecord.Books) {
            $book | Add-Member -NotePropertyName PreFilterExcluded -NotePropertyValue $false -Force
        }

        $forceInclude = [bool](Get-ObjectPropertyValue -InputObject $authorRecord -Name 'ForceInclude')
        if ($forceInclude) {
            foreach ($book in $authorRecord.Books) {
                [void]$candidates.Add($book)
            }
            Write-RunLog -Level INFO -Message (
                "Force-included '$($authorRecord.Author)': bypassed ratings prefilter for $($authorRecord.Books.Count) canonical book(s)."
            )
            continue
        }

        if ($FullMetadataScan) {
            foreach ($book in $authorRecord.Books) {
                [void]$candidates.Add($book)
            }
            continue
        }

        $standalones = @($authorRecord.Books | Where-Object {
            [string]::IsNullOrWhiteSpace([string]$_.SeriesName)
        })
        foreach ($standalone in $standalones) {
            $mayHaveUnparsedSeries = Test-PossibleSeriesTitle -Title ([string]$standalone.CompleteTitle)
            if (($standalone.AverageRating -ge $MinimumRating -and
                    $standalone.RatingCount -ge $MinimumRatingsCount) -or
                $mayHaveUnparsedSeries) {
                [void]$candidates.Add($standalone)
                continue
            }

            $standalone.PreFilterExcluded = $true
            Add-Exclusion -Reason 'Standalone below rating or ratings-count threshold' -Author $standalone.Author -Title $standalone.Title
        }

        $seriesBooks = @($authorRecord.Books | Where-Object {
            -not [string]::IsNullOrWhiteSpace([string]$_.SeriesName)
        })
        foreach ($seriesGroup in ($seriesBooks | Group-Object -Property {
            Get-NormalizedSeriesKey -SeriesName ([string]$_.SeriesName)
        })) {
            $books = @($seriesGroup.Group)

            # Detail-page validation can remove a no-page, audio, translated, or
            # incorrectly attributed edition. A series is therefore rejected here
            # only when no possible remaining subset could pass both thresholds.
            $couldMeetAverage = @($books | Where-Object {
                $_.AverageRating -ge $MinimumRating
            }).Count -gt 0
            $couldHaveQualifyingFirstBook = @($books | Where-Object {
                $_.RatingCount -ge $MinimumRatingsCount
            }).Count -gt 0

            if ($couldMeetAverage -and $couldHaveQualifyingFirstBook) {
                foreach ($book in $books) {
                    [void]$candidates.Add($book)
                }
                continue
            }

            foreach ($book in $books) {
                $book.PreFilterExcluded = $true
                Add-Exclusion -Reason 'Series below average or first-book ratings-count threshold' -Author $book.Author -Title $book.Title
            }
        }
    }

    return @($candidates)
}

function Get-FilteredAuthorResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [psobject]$AuthorRecord,

        [Parameter(Mandatory)]
        [hashtable]$DetailByBookId
    )

    $forceInclude = [bool](Get-ObjectPropertyValue -InputObject $AuthorRecord -Name 'ForceInclude')
    $usableBooks = [System.Collections.Generic.List[object]]::new()
    foreach ($book in $AuthorRecord.Books) {
        if ([bool](Get-ObjectPropertyValue -InputObject $book -Name 'PreFilterExcluded')) {
            continue
        }

        if ($AuthorListOnly) {
            $book | Add-Member -NotePropertyName Pages -NotePropertyValue $null -Force
            $book | Add-Member -NotePropertyName SeriesAverage -NotePropertyValue $null -Force
            [void]$usableBooks.Add($book)
            continue
        }

        $bookId = [string]$book.BookId
        if (-not $DetailByBookId.ContainsKey($bookId)) {
            Add-Exclusion -Reason 'Book-detail request or parse failure' -Author $book.Author -Title $book.Title
            continue
        }

        $detail = $DetailByBookId[$bookId]
        if (-not (Test-UsableBookDetail -Book $book -Detail $detail)) {
            continue
        }

        $book | Add-Member -NotePropertyName Pages -NotePropertyValue ([int]$detail.Pages) -Force
        if (-not [string]::IsNullOrWhiteSpace([string]$detail.DetailTitle)) {
            $book.Title = [string]$detail.DetailTitle
        }
        $detailSeriesName = [string]$detail.SeriesName
        if (-not [string]::IsNullOrWhiteSpace($detailSeriesName)) {
            if ([string]::IsNullOrWhiteSpace([string]$book.SeriesName)) {
                $book.SeriesId = [string]$detail.SeriesId
                $book.SeriesName = $detailSeriesName
                $book.SeriesNumber = [string]$detail.SeriesNumber
            }
            elseif ((Get-NormalizedSeriesKey -SeriesName ([string]$book.SeriesName)) -eq
                (Get-NormalizedSeriesKey -SeriesName $detailSeriesName)) {
                if ([string]::IsNullOrWhiteSpace([string]$book.SeriesId)) {
                    $book.SeriesId = [string]$detail.SeriesId
                }
                if ([string]::IsNullOrWhiteSpace([string]$book.SeriesNumber)) {
                    $book.SeriesNumber = [string]$detail.SeriesNumber
                }
            }
            else {
                Write-RunLog -Level DEBUG -Message (
                    "Preserved author-list series '$($book.SeriesName)' for '$($book.Title)' instead of overlapping detail-page series '$detailSeriesName'."
                )
            }
        }
        $book | Add-Member -NotePropertyName SeriesAverage -NotePropertyValue $null -Force
        [void]$usableBooks.Add($book)
    }

    # Detail-page display titles can converge even when author-list titles differ.
    # Recheck here so the final output still contains one title per author.
    $usableBooks = @(Get-UniqueTitleBookList -Books @($usableBooks) -AuthorName $AuthorRecord.Author)

    $survivors = [System.Collections.Generic.List[object]]::new()

    foreach ($standalone in @($usableBooks | Where-Object { [string]::IsNullOrWhiteSpace($_.SeriesName) })) {
        if ($forceInclude -or
            ($standalone.AverageRating -ge $MinimumRating -and
                $standalone.RatingCount -ge $MinimumRatingsCount)) {
            [void]$survivors.Add($standalone)
        }
        else {
            Add-Exclusion -Reason 'Standalone below rating or ratings-count threshold' -Author $standalone.Author -Title $standalone.Title
        }
    }

    $seriesBooks = @($usableBooks | Where-Object { -not [string]::IsNullOrWhiteSpace($_.SeriesName) })
    foreach ($seriesGroup in ($seriesBooks | Group-Object -Property {
        Get-NormalizedSeriesKey -SeriesName ([string]$_.SeriesName)
    })) {
        $books = @($seriesGroup.Group)
        $firstBook = Get-SeriesFirstBook -Books $books
        $canonicalSeriesName = [string]$firstBook.SeriesName
        $seriesAverage = Get-UnweightedAverage -Values @($books | ForEach-Object { $_.AverageRating })

        if ($forceInclude -or
            ($seriesAverage -ge $MinimumRating -and
                $firstBook.RatingCount -ge $MinimumRatingsCount)) {
            $roundedSeriesAverage = [math]::Round($seriesAverage, 3, [System.MidpointRounding]::AwayFromZero)
            foreach ($seriesBook in $books) {
                $seriesBook.SeriesName = $canonicalSeriesName
                $seriesBook.SeriesAverage = $roundedSeriesAverage
                [void]$survivors.Add($seriesBook)
            }
        }
        else {
            foreach ($seriesBook in $books) {
                Add-Exclusion -Reason 'Series below average or first-book ratings-count threshold' -Author $seriesBook.Author -Title $seriesBook.Title
            }
        }
    }

    if ($survivors.Count -eq 0) {
        if ($forceInclude) {
            Write-RunLog -Level WARN -Message (
                "Force-inclusion could not retain '$($AuthorRecord.Author)' because no books survived non-rating eligibility checks."
            )
        }
        else {
            Write-RunLog -Level WARN -Message "No books survived filtering for '$($AuthorRecord.Author)'."
        }
        return $null
    }

    if ($forceInclude) {
        Write-RunLog -Level INFO -Message (
            "Force-included '$($AuthorRecord.Author)' with $($survivors.Count) book(s) after non-rating eligibility checks."
        )
    }

    $authorAverageRaw = Get-UnweightedAverage -Values @($survivors | ForEach-Object { $_.AverageRating })
    $authorAverage = [math]::Round($authorAverageRaw, 3, [System.MidpointRounding]::AwayFromZero)

    return [pscustomobject]@{
        AuthorId      = $AuthorRecord.AuthorId
        Author        = $AuthorRecord.Author
        Genre         = $AuthorRecord.Genre
        ForceInclude  = $forceInclude
        AuthorAverage = $authorAverage
        Books         = @($survivors)
    }
}

function Get-GenreBucket {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Genre
    )

    $normalized = ([regex]::Replace($Genre.Trim(), '[-_\s]+', ' ')).ToUpperInvariant()
    if ($normalized -eq 'FANTASY') {
        return 'Fantasy'
    }
    if ($normalized -in @('SCIENCE FICTION', 'SCI FI', 'SCIFI')) {
        return 'ScienceFiction'
    }
    return 'Other'
}

function Get-RankedAuthorList {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Authors,

        [Parameter(Mandatory)]
        [ValidateRange(1, 4)]
        [int]$OrderNumber
    )

    $buckets = @{
        Fantasy       = @()
        ScienceFiction = @()
        Other          = @()
    }

    foreach ($author in $Authors) {
        $bucketName = Get-GenreBucket -Genre $author.Genre
        $buckets[$bucketName] += $author
    }

    foreach ($bucketName in @('Fantasy', 'ScienceFiction', 'Other')) {
        $buckets[$bucketName] = @($buckets[$bucketName] | Sort-Object -Property (
            @{ Expression = 'AuthorAverage'; Descending = $true },
            @{ Expression = { $_.Author.ToUpperInvariant() }; Ascending = $true }
        ))
    }

    $patterns = @{
        1 = @('Fantasy', 'ScienceFiction', 'Fantasy', 'Other')
        2 = @('ScienceFiction', 'Fantasy', 'Other', 'Fantasy')
        3 = @('Fantasy', 'Other', 'Fantasy', 'ScienceFiction')
        4 = @('Other', 'Fantasy', 'ScienceFiction', 'Fantasy')
    }
    $pattern = $patterns[$OrderNumber]
    $positions = @{ Fantasy = 0; ScienceFiction = 0; Other = 0 }
    $ranked = [System.Collections.Generic.List[object]]::new()
    $rank = 1

    while ($ranked.Count -lt $Authors.Count) {
        $addedThisCycle = 0
        foreach ($bucketName in $pattern) {
            $position = $positions[$bucketName]
            if ($position -ge $buckets[$bucketName].Count) {
                continue
            }

            $author = $buckets[$bucketName][$position]
            $author | Add-Member -NotePropertyName OverallRank -NotePropertyValue $rank -Force
            [void]$ranked.Add($author)
            $positions[$bucketName]++
            $rank++
            $addedThisCycle++
        }

        if ($addedThisCycle -eq 0) {
            throw 'Author ranking made no progress; bucket state is inconsistent.'
        }
    }

    return @($ranked)
}

function Get-SortedBookList {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$RankedAuthors
    )

    $allBooks = [System.Collections.Generic.List[object]]::new()
    foreach ($author in $RankedAuthors) {
        foreach ($book in $author.Books) {
            $book | Add-Member -NotePropertyName OverallRank -NotePropertyValue $author.OverallRank -Force
            $book | Add-Member -NotePropertyName AuthorAverage -NotePropertyValue $author.AuthorAverage -Force
            $blockKey = if (-not [string]::IsNullOrWhiteSpace($book.SeriesName)) {
                "series:$(Get-NormalizedSeriesKey -SeriesName ([string]$book.SeriesName))"
            }
            else {
                "standalone:$($book.WorkKey)"
            }
            $blockSortName = if (-not [string]::IsNullOrWhiteSpace($book.SeriesName)) {
                $book.SeriesName
            }
            else {
                $book.Title
            }
            $book | Add-Member -NotePropertyName BlockKey -NotePropertyValue $blockKey -Force
            $book | Add-Member -NotePropertyName BlockSortName -NotePropertyValue $blockSortName -Force
            [void]$allBooks.Add($book)
        }
    }

    foreach ($block in ($allBooks | Group-Object -Property { "$($_.AuthorId)|$($_.BlockKey)" })) {
        $knownYears = @($block.Group | Where-Object { $null -ne $_.PublicationYear } | ForEach-Object { [int]$_.PublicationYear })
        $earliestYear = if ($knownYears.Count -gt 0) {
            ($knownYears | Measure-Object -Minimum).Minimum
        }
        else {
            9999
        }

        foreach ($book in $block.Group) {
            $book | Add-Member -NotePropertyName BlockEarliestYear -NotePropertyValue $earliestYear -Force
            $book | Add-Member -NotePropertyName SeriesNumberSortKey -NotePropertyValue (
                Get-SeriesNumberSortKey -SeriesNumber $book.SeriesNumber
            ) -Force
            $publicationYearSortKey = if ($null -ne $book.PublicationYear) {
                [int]$book.PublicationYear
            }
            else {
                9999
            }
            $book | Add-Member -NotePropertyName PublicationYearSortKey -NotePropertyValue $publicationYearSortKey -Force
        }
    }

    return @($allBooks | Sort-Object -Property (
        @{ Expression = 'OverallRank'; Ascending = $true },
        @{ Expression = 'BlockEarliestYear'; Ascending = $true },
        @{ Expression = { $_.BlockSortName.ToUpperInvariant() }; Ascending = $true },
        @{ Expression = 'SeriesNumberSortKey'; Ascending = $true },
        @{ Expression = 'PublicationYearSortKey'; Ascending = $true },
        @{ Expression = { $_.Title.ToUpperInvariant() }; Ascending = $true }
    ))
}

function Get-NextReadingDay {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [datetime]$Date
    )

    $candidate = $Date.Date
    while ($candidate.DayOfWeek -in @([DayOfWeek]::Friday, [DayOfWeek]::Saturday)) {
        $candidate = $candidate.AddDays(1)
    }
    return $candidate
}

function Get-ReadingCompletionDate {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [datetime]$Start,

        [Parameter(Mandatory)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]$ReadingDays
    )

    $date = Get-NextReadingDay -Date $Start
    $daysCounted = 0
    while ($daysCounted -lt $ReadingDays) {
        if ($date.DayOfWeek -notin @([DayOfWeek]::Friday, [DayOfWeek]::Saturday)) {
            $daysCounted++
            if ($daysCounted -eq $ReadingDays) {
                return $date
            }
        }
        $date = $date.AddDays(1)
    }

    throw 'Reading-date calculation failed unexpectedly.'
}

function Get-SecondSundayAfter {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [datetime]$Date
    )

    $dayNumber = [int]$Date.DayOfWeek
    $daysToNextSunday = (7 - $dayNumber) % 7
    if ($daysToNextSunday -eq 0) {
        $daysToNextSunday = 7
    }
    return $Date.Date.AddDays($daysToNextSunday + 7)
}

function Add-ReadingSchedule {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Books
    )

    foreach ($book in $Books) {
        $book | Add-Member -NotePropertyName EstimatedStartDate -NotePropertyValue $null -Force
        $book | Add-Member -NotePropertyName EstimatedEndDate -NotePropertyValue $null -Force
    }

    if ($AuthorListOnly) {
        Write-RunLog -Level INFO -Message 'Reading-date estimation skipped in author-list-only mode.'
        return $Books
    }

    $nextStart = Get-NextReadingDay -Date $StartDate
    $schedulingStopped = $false

    foreach ($book in $Books) {
        if ($schedulingStopped) {
            continue
        }

        $readingDays = [int][math]::Ceiling([double]$book.Pages / $PagesPerReadingDay)
        $completionDate = Get-ReadingCompletionDate -Start $nextStart -ReadingDays $readingDays
        if ($completionDate.Year -ge $ScheduleCutoffYear) {
            $schedulingStopped = $true
            Write-RunLog -Level INFO -Message (
                "Scheduling stopped before '$($book.Title)': estimated completion $($completionDate.ToString('yyyy-MM-dd')) reaches cutoff year $ScheduleCutoffYear."
            )
            continue
        }

        $book.EstimatedStartDate = $nextStart
        $book.EstimatedEndDate = $completionDate
        $nextStart = Get-SecondSundayAfter -Date $completionDate
    }

    return $Books
}

function Export-ReadingPlan {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Books,

        [Parameter(Mandatory)]
        [string]$LiteralPath
    )

    $rows = foreach ($book in $Books) {
        [pscustomobject][ordered]@{
            'Overall Rank'                = [int]$book.OverallRank
            'Author'                      = Protect-CsvText ([string]$book.Author)
            'Genre'                       = Protect-CsvText ([string]$book.Genre)
            'Author Average'              = Format-Decimal -Value $book.AuthorAverage -MaximumDecimals 3
            'Title'                       = Protect-CsvText ([string]$book.Title)
            'Series Name'                 = Protect-CsvText ([string]$book.SeriesName)
            'Series Number'               = Protect-CsvText ([string]$book.SeriesNumber)
            'Series Average'              = if ($null -ne $book.SeriesAverage) {
                Format-Decimal -Value $book.SeriesAverage -MaximumDecimals 3
            } else { '' }
            'Publication Year'            = if ($null -ne $book.PublicationYear) { [int]$book.PublicationYear } else { '' }
            'Pages'                       = if ($null -ne $book.Pages) { [int]$book.Pages } else { '' }
            'Average book rating'         = Format-Decimal -Value $book.AverageRating -MaximumDecimals 2
            'Review count'                = [long]$book.RatingCount
            'Estimated Reading Start Date' = if ($null -ne $book.EstimatedStartDate) {
                $book.EstimatedStartDate.ToString('yyyy-MM-dd', $script:InvariantCulture)
            } else { '' }
            'Estimated Reading End Date'  = if ($null -ne $book.EstimatedEndDate) {
                $book.EstimatedEndDate.ToString('yyyy-MM-dd', $script:InvariantCulture)
            } else { '' }
        }
    }

    $parent = Split-Path -Parent $LiteralPath
    if (-not (Test-Path -LiteralPath $parent -PathType Container)) {
        [void](New-Item -ItemType Directory -Path $parent -Force)
    }

    $temporaryPath = Join-Path -Path $parent -ChildPath (
        '.{0}.{1}.tmp' -f ([System.IO.Path]::GetFileName($LiteralPath)), ([guid]::NewGuid().ToString('N'))
    )

    try {
        @($rows) | Export-Csv -LiteralPath $temporaryPath -NoTypeInformation -Encoding utf8BOM
        [System.IO.File]::Move($temporaryPath, $LiteralPath, $true)
    }
    finally {
        if (Test-Path -LiteralPath $temporaryPath -PathType Leaf) {
            Remove-Item -LiteralPath $temporaryPath -Force -ErrorAction SilentlyContinue
        }
    }
}

# Main
$InputCsv = (Resolve-Path -LiteralPath $InputCsv).Path
$OutputCsv = Get-UnresolvedFullPath -Path $OutputCsv
if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = [System.IO.Path]::ChangeExtension($OutputCsv, '.log')
}
else {
    $LogPath = Get-UnresolvedFullPath -Path $LogPath
}

if ([System.StringComparer]::OrdinalIgnoreCase.Equals($InputCsv, $OutputCsv)) {
    throw 'InputCsv and OutputCsv must be different files.'
}
if ([System.StringComparer]::OrdinalIgnoreCase.Equals($InputCsv, $LogPath)) {
    throw 'InputCsv and LogPath must be different files.'
}
if ([System.StringComparer]::OrdinalIgnoreCase.Equals($OutputCsv, $LogPath)) {
    throw 'OutputCsv and LogPath must be different files.'
}
if ($AuthorListOnly -and $FullMetadataScan) {
    throw 'AuthorListOnly and FullMetadataScan cannot be used together.'
}

$script:LogPath = $LogPath
$logParent = Split-Path -Parent $script:LogPath
if (-not (Test-Path -LiteralPath $logParent -PathType Container)) {
    [void](New-Item -ItemType Directory -Path $logParent -Force)
}

$orderMatch = [regex]::Match($Order, '(?<number>[1-4])')
$orderNumber = [int]$orderMatch.Groups['number'].Value
$startedUtc = [datetime]::UtcNow

try {
    Write-RunLog -Level INFO -Message ('=' * 72)
    Write-RunLog -Level INFO -Message "Run started. Input='$InputCsv'; Output='$OutputCsv'; Order=$orderNumber."
    if ($AuthorListOnly) {
        Write-RunLog -Level INFO -Message (
            "Thresholds: rating >= $MinimumRating; ratings count >= $MinimumRatingsCount. Page counts and scheduling are disabled."
        )
    }
    else {
        Write-RunLog -Level INFO -Message (
            "Thresholds: rating >= $MinimumRating; ratings count >= $MinimumRatingsCount; pages/day=$PagesPerReadingDay; cutoff year=$ScheduleCutoffYear."
        )
    }
    $scanMode = if ($AuthorListOnly) {
        'author-list only; no individual book pages, page counts, or reading dates'
    }
    elseif ($FullMetadataScan) {
        'full book-detail scan'
    }
    else {
        'prefiltered book-detail scan'
    }
    Write-RunLog -Level INFO -Message "Metadata scan mode: $scanMode. Caching is disabled."
    Write-RunLog -Level INFO -Message (
        "Author-list pagination has no fixed page limit. After processing a page, its next page is skipped when any listed book has fewer than $MinimumRatingsCount ratings, except for authors marked Include=Y."
    )

    New-GoodreadsHttpClient
    $configs = Import-AuthorConfiguration -LiteralPath $InputCsv
    Write-RunLog -Level INFO -Message "Loaded $($configs.Count) unique author configuration(s)."

    $authorRecords = [System.Collections.Generic.List[object]]::new()
    foreach ($config in $configs) {
        try {
            $authorRecord = Get-GoodreadsAuthorRecord -Config $config
            [void]$authorRecords.Add($authorRecord)
            Write-RunLog -Level INFO -Message "Collected $($authorRecord.Books.Count) canonical candidate work(s) for '$($authorRecord.Author)'."
        }
        catch {
            Write-RunLog -Level ERROR -Message "Author ID $($config.AuthorId) failed and will be skipped: $($_.Exception.Message)"
        }
    }

    if ($authorRecords.Count -eq 0) {
        throw 'No author pages were collected successfully.'
    }

    $allUniqueBooks = @(
        $authorRecords.Books |
        Group-Object -Property BookId |
        ForEach-Object { $_.Group[0] }
    )

    $detailCandidates = @(Get-DetailCandidateBookList -AuthorRecords @($authorRecords))
    $detailByBookId = @{}
    if ($AuthorListOnly) {
        Write-RunLog -Level INFO -Message (
            "Skipped individual detail pages for all $($allUniqueBooks.Count) canonical book(s)."
        )
    }
    else {
        $uniqueBooks = @(
            $detailCandidates |
            Group-Object -Property BookId |
            ForEach-Object { $_.Group[0] }
        )
        $savedRequests = $allUniqueBooks.Count - $uniqueBooks.Count
        Write-RunLog -Level INFO -Message (
            "Author-list prefilter reduced detail-page candidates from $($allUniqueBooks.Count) to $($uniqueBooks.Count), avoiding $savedRequests request(s)."
        )
        $detailByBookId = Get-BookDetailMap -Books $uniqueBooks
    }

    $filteredAuthors = [System.Collections.Generic.List[object]]::new()
    foreach ($authorRecord in $authorRecords) {
        $filtered = Get-FilteredAuthorResult -AuthorRecord $authorRecord -DetailByBookId $detailByBookId
        if ($null -ne $filtered) {
            [void]$filteredAuthors.Add($filtered)
            Write-RunLog -Level INFO -Message (
                "'$($filtered.Author)' retained $($filtered.Books.Count) book(s); author average $($filtered.AuthorAverage)."
            )
        }
    }

    if ($filteredAuthors.Count -eq 0) {
        throw 'No books survived filtering. Review the log for exclusions or Goodreads parsing failures.'
    }

    $rankedAuthors = Get-RankedAuthorList -Authors @($filteredAuthors) -OrderNumber $orderNumber
    $sortedBooks = Get-SortedBookList -RankedAuthors $rankedAuthors
    $scheduledBooks = Add-ReadingSchedule -Books $sortedBooks
    Export-ReadingPlan -Books $scheduledBooks -LiteralPath $OutputCsv

    $elapsed = [datetime]::UtcNow - $startedUtc
    Write-RunLog -Level INFO -Message (
        "Completed successfully: $($rankedAuthors.Count) author(s), $($scheduledBooks.Count) book(s), elapsed $($elapsed.ToString())."
    )
    foreach ($reason in ($script:ExclusionCounts.Keys | Sort-Object)) {
        Write-RunLog -Level INFO -Message "Exclusions [$reason]: $($script:ExclusionCounts[$reason])"
    }

    Write-Output "Created reading plan: $OutputCsv"
    Write-Output "Log file: $LogPath"
}
catch {
    try {
        Write-RunLog -Level ERROR -Message "Fatal error: $($_.Exception.Message)"
    }
    catch {
        Write-Warning "Fatal error (logging also failed): $($_.Exception.Message)"
    }
    throw
}
finally {
    if ($null -ne $script:HttpClient) {
        $script:HttpClient.Dispose()
    }
}