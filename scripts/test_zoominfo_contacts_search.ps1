param(
  [string]$FirstName = "Jason",
  [string]$LastName = "",
  [int]$PageSize = 10,
  [string]$BaseUrl = "https://api.zoominfo.com/gtm",
  [string]$Token = $env:ZOOMINFO_ACCESS_TOKEN,
  [switch]$IncludeFullName = $true,
  [switch]$IncludeFirstName = $true
)

if ([string]::IsNullOrWhiteSpace($Token)) {
  Write-Error "ZOOMINFO_ACCESS_TOKEN is not set. Set it in your environment or pass -Token <token>."
  exit 1
}

$url = "$BaseUrl/data/v1/contacts/search?page[number]=1&page[size]=$PageSize"

$attrs = @{}
$attrs["firstName"] = $FirstName
$fullName = if ([string]::IsNullOrWhiteSpace($LastName)) { $FirstName } else { "$FirstName $LastName" }
$attrs["fullName"] = $fullName

$attrsToSend = @{}
if ($IncludeFirstName) { $attrsToSend["firstName"] = $attrs["firstName"] }
if ($IncludeFullName) { $attrsToSend["fullName"] = $attrs["fullName"] }

if ($attrsToSend.Keys.Count -eq 0) {
  Write-Error "IncludeFirstName/IncludeFullName resulted in an empty attributes object."
  exit 1
}

$bodyObj = @{
  data = @{
    type = "ContactSearch"
    attributes = $attrsToSend
  }
}
$jsonBody = $bodyObj | ConvertTo-Json -Depth 10 -Compress

$headers = @(
  "--header", "accept: application/vnd.api+json",
  "--header", "content-type: application/vnd.api+json",
  "--header", "Authorization: Bearer $Token"
)

$tmp = Join-Path $env:TEMP ("zoominfo_contact_search_" + [Guid]::NewGuid().ToString() + ".json")

$cmd = @(
  "curl.exe",
  "--silent",
  "--show-error",
  "--location",
  "--request", "POST",
  "--url", $url
) + $headers + @(
  "--data", $jsonBody,
  "--output", $tmp,
  "--write-out", "%{http_code}"
)

$httpCode = & $cmd

Write-Host "HTTP $httpCode"
if (Test-Path $tmp) {
  $text = Get-Content -Raw -Path $tmp
  if ($null -ne $text -and $text.Length -gt 0) {
    $preview = if ($text.Length -gt 2000) { $text.Substring(0,2000) + "...(truncated)" } else { $text }
    Write-Host $preview
  } else {
    Write-Host "(empty response)"
  }
  Remove-Item -Force -Path $tmp -ErrorAction SilentlyContinue
}

