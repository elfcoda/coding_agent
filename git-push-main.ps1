[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$CommitMessage
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($CommitMessage)) {
    throw "Commit message cannot be empty."
}

Set-Location -Path $PSScriptRoot

git add .
if ($LASTEXITCODE -ne 0) {
    throw "git add failed."
}

git commit -m $CommitMessage
if ($LASTEXITCODE -ne 0) {
    throw "git commit failed."
}

git push origin main
if ($LASTEXITCODE -ne 0) {
    throw "git push failed."
}
