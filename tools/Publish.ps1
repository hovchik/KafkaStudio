#requires -Version 5.1
<#
	Builds a distributable, self-contained win-x64 publish of KafkaStudio and (if Inno Setup's
	ISCC.exe is installed and on PATH, or found at its default install location) compiles it into a
	Windows installer via installer\KafkaStudio.iss.

	Usage:
		pwsh tools\Publish.ps1
		pwsh tools\Publish.ps1 -Version 1.2.0
		pwsh tools\Publish.ps1 -SkipInstaller   # just publish, don't build the .exe installer
#>
param(
	[string]$Version = "1.0.0",
	[switch]$SkipInstaller
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$publishDir = Join-Path $repoRoot "publish\win-x64"
$projectPath = Join-Path $repoRoot "src\KafkaStudio.App\KafkaStudio.App.csproj"

Write-Host "Publishing KafkaStudio $Version (win-x64, self-contained)..." -ForegroundColor Cyan
if (Test-Path $publishDir) {
	Remove-Item -Recurse -Force $publishDir
}

dotnet publish $projectPath `
	-c Release `
	-r win-x64 `
	--self-contained true `
	-p:PublishSingleFile=false `
	-p:Version=$Version `
	-o $publishDir

if ($LASTEXITCODE -ne 0) {
	throw "dotnet publish failed with exit code $LASTEXITCODE"
}

Write-Host "Published to $publishDir" -ForegroundColor Green

if ($SkipInstaller) {
	return
}

$iscc = Get-Command "ISCC.exe" -ErrorAction SilentlyContinue
if (-not $iscc) {
	$defaultPath = "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe"
	if (Test-Path $defaultPath) {
		$iscc = Get-Item $defaultPath
	}
}

if (-not $iscc) {
	Write-Warning "Inno Setup's ISCC.exe was not found. Install Inno Setup 6 (https://jrsoftware.org/isdl.php) and re-run this script, or compile installer\KafkaStudio.iss manually."
	return
}

Write-Host "Compiling installer with Inno Setup..." -ForegroundColor Cyan
$env:KAFKASTUDIO_VERSION = $Version
& $iscc.Source (Join-Path $repoRoot "installer\KafkaStudio.iss")

if ($LASTEXITCODE -ne 0) {
	throw "ISCC.exe failed with exit code $LASTEXITCODE"
}

Write-Host "Installer written to installer\output\KafkaStudioSetup-$Version.exe" -ForegroundColor Green
