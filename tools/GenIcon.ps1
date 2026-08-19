#requires -Version 5.1
Add-Type -AssemblyName System.Drawing

$sizes = @(16, 32, 48, 256)
$repoRoot = Split-Path -Parent $PSScriptRoot
$assetsDir = Join-Path $repoRoot "src\KafkaStudio.App\Assets"
New-Item -ItemType Directory -Force -Path $assetsDir | Out-Null
$icoPath = Join-Path $assetsDir "app.ico"

function New-IconPngBytes {
	param([int]$size)

	$bmp = New-Object System.Drawing.Bitmap $size, $size
	$g = [System.Drawing.Graphics]::FromImage($bmp)
	$g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
	$g.Clear([System.Drawing.Color]::Transparent)

	$bgColor = [System.Drawing.Color]::FromArgb(255, 27, 31, 42)
	$bgBrush = New-Object System.Drawing.SolidBrush $bgColor
	$radius = [Math]::Max(2, [int]($size * 0.18))
	$rect = New-Object System.Drawing.Rectangle 0, 0, $size, $size
	$path = New-Object System.Drawing.Drawing2D.GraphicsPath
	$d = $radius * 2
	$path.AddArc($rect.X, $rect.Y, $d, $d, 180, 90)
	$path.AddArc($rect.Right - $d, $rect.Y, $d, $d, 270, 90)
	$path.AddArc($rect.Right - $d, $rect.Bottom - $d, $d, $d, 0, 90)
	$path.AddArc($rect.X, $rect.Bottom - $d, $d, $d, 90, 90)
	$path.CloseFigure()
	$g.FillPath($bgBrush, $path)

	$barColors = @(
		[System.Drawing.Color]::FromArgb(255, 76, 217, 123),
		[System.Drawing.Color]::FromArgb(255, 86, 156, 214),
		[System.Drawing.Color]::FromArgb(255, 224, 175, 104)
	)
	$heightFactors = @(0.55, 1.0, 0.75)
	$barCount = 3
	$margin = $size * 0.20
	$gap = $size * 0.10
	$barWidth = ($size - 2 * $margin - ($barCount - 1) * $gap) / $barCount
	$maxBarHeight = $size * 0.50
	$baseline = $size * 0.78

	for ($i = 0; $i -lt $barCount; $i++) {
		$barHeight = $maxBarHeight * $heightFactors[$i]
		$x = $margin + $i * ($barWidth + $gap)
		$y = $baseline - $barHeight
		$brush = New-Object System.Drawing.SolidBrush $barColors[$i]
		$barRadius = [Math]::Max(1, $barWidth * 0.25)
		$barRect = New-Object System.Drawing.RectangleF $x, $y, $barWidth, $barHeight
		$barPath = New-Object System.Drawing.Drawing2D.GraphicsPath
		$bd = $barRadius * 2
		$barPath.AddArc($barRect.X, $barRect.Y, $bd, $bd, 180, 90)
		$barPath.AddArc($barRect.Right - $bd, $barRect.Y, $bd, $bd, 270, 90)
		$barPath.AddArc($barRect.Right - $bd, $barRect.Bottom - $bd, $bd, $bd, 0, 90)
		$barPath.AddArc($barRect.X, $barRect.Bottom - $bd, $bd, $bd, 90, 90)
		$barPath.CloseFigure()
		$g.FillPath($brush, $barPath)
		$brush.Dispose()
		$barPath.Dispose()
	}

	$ms = New-Object System.IO.MemoryStream
	$bmp.Save($ms, [System.Drawing.Imaging.ImageFormat]::Png)
	$bytes = $ms.ToArray()

	$g.Dispose()
	$bgBrush.Dispose()
	$path.Dispose()
	$ms.Dispose()
	$bmp.Dispose()

	return , $bytes
}

$frames = New-Object System.Collections.ArrayList
foreach ($size in $sizes) {
	$bytes = New-IconPngBytes -size $size
	[void]$frames.Add(@{ Size = $size; Bytes = $bytes })
}

$fs = [System.IO.File]::Open($icoPath, [System.IO.FileMode]::Create)
$writer = New-Object System.IO.BinaryWriter $fs

$writer.Write([UInt16]0)
$writer.Write([UInt16]1)
$writer.Write([UInt16]$frames.Count)

$dataOffset = 6 + (16 * $frames.Count)
foreach ($frame in $frames) {
	$frame.Offset = $dataOffset
	$dataOffset += $frame.Bytes.Length
}

foreach ($frame in $frames) {
	$size = [int]$frame.Size
	$wb = if ($size -ge 256) { 0 } else { $size }
	$hb = if ($size -ge 256) { 0 } else { $size }
	$writer.Write([Byte]$wb)
	$writer.Write([Byte]$hb)
	$writer.Write([Byte]0)
	$writer.Write([Byte]0)
	$writer.Write([UInt16]1)
	$writer.Write([UInt16]32)
	$writer.Write([UInt32]$frame.Bytes.Length)
	$writer.Write([UInt32]$frame.Offset)
}

foreach ($frame in $frames) {
	[byte[]]$b = $frame.Bytes
	$writer.Write($b)
}

$writer.Flush()
$writer.Dispose()
$fs.Dispose()

Write-Host "Wrote icon: $icoPath"
