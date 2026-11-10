param(
    [Parameter(Mandatory=$true)][string]$InputPptx,
    [Parameter(Mandatory=$true)][string]$OutputDirectory
)
$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$powerpoint = New-Object -ComObject PowerPoint.Application
$initialCount = $powerpoint.Presentations.Count
$presentation = $null
try {
    $presentation = $powerpoint.Presentations.Open($InputPptx, -1, 0, 0)
    $presentation.Export($OutputDirectory, 'PNG', 1600, 900)
    $pdf = Join-Path (Split-Path $OutputDirectory -Parent) (([System.IO.Path]::GetFileNameWithoutExtension($InputPptx)) + '.pdf')
    $presentation.SaveAs($pdf, 32)
    Write-Output ('Rendered slides: ' + $presentation.Slides.Count)
    Write-Output ('PDF: ' + $pdf)
} finally {
    if ($null -ne $presentation) { $presentation.Close() }
    if ($initialCount -eq 0) { $powerpoint.Quit() }
}
