# apply_patch_win.ps1
# Apply patch from clipboard, commit and push to origin/main (Windows).
# Robust to Markdown fences and invisible leading chars (BOM/ZWSP/LRM/RLM/NBSP).
# Supports git-format-patch (git am) and unified diff (git apply).

$ErrorActionPreference = "Stop"

function Run-Git([string]$ArgsLine) {
  & git $ArgsLine
  if ($LASTEXITCODE -ne 0) {
    throw "[GIT] Command failed: git $ArgsLine"
  }
}

git rev-parse --is-inside-work-tree | Out-Null

# 1) Clipboard
$raw = Get-Clipboard
if ([string]::IsNullOrWhiteSpace($raw)) { throw "[ERR] Clipboard is empty." }

# 2) Normalize
$raw = $raw -replace "`r`n","`n" -replace "`r","`n"
$raw = $raw -replace "[\uFEFF\u200B\u200E\u200F\u00A0]",""
$raw = $raw.Trim()

# 3) Strip Markdown fences
$raw = $raw -replace '(?s)^\s*```[a-zA-Z0-9_-]*\s*',''
$raw = $raw -replace '(?s)\s*```\s*$',''
$raw = $raw.Trim()

# 4) Keep from first 'From <sha>' or 'diff --git'
$from = [regex]::Match($raw,'(?m)^\s*From [0-9a-f]{40}\b')
$diff = [regex]::Match($raw,'(?m)^\s*diff --git\s')
if ($from.Success) {
  $raw = $raw.Substring($from.Index)
} elseif ($diff.Success) {
  $raw = $raw.Substring($diff.Index)
} else {
  # synthesize header if we see +++/---
  $plus  = [regex]::Match($raw,'(?m)^\s*\+\+\+\s+b/([^\r\n]+)')
  if ($plus.Success) {
    $path = $plus.Groups[1].Value.Trim()
    $raw  = "diff --git a/$path b/$path`n$raw"
  }
}

# 5) Basic validation: require at least one hunk '@@'
if ($raw -notmatch '(?m)^@@ ') {
  throw "[ERR] No hunks found ('@@ ... @@'). The clipboard likely contains headers only or an incomplete patch."
}

# 6) Save temp (UTF-8 no BOM)
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[IO.File]::WriteAllText($tmp,$raw,[Text.UTF8Encoding]::new($false))
Write-Host "[i] Patch saved to $tmp"

# 7) Diagnostics (first lines)
$lines = $raw -split "`n"
$preview = ($lines | Select-Object -First 4) -join "`n"
Write-Host "[i] First lines:`n$preview"

# 8) Detect format
$gitFormat = ($raw -match '(?m)^\s*From [0-9a-f]{40}\b' -and $raw -match '(?m)^\s*Subject:')
$unified   = ($raw -match '(?m)^\s*diff --git\s')

# 9) Apply
if ($gitFormat) {
  Write-Host "[i] Trying: git am -3"
  Run-Git "am -3 `"$tmp`""
  Write-Host "[OK] Applied via git am"
} else {
  Write-Host "[i] Trying: git apply --check/--whitespace=fix"
  Run-Git "apply --check --whitespace=fix `"$tmp`""
  Run-Git "apply --whitespace=fix `"$tmp`""
  Run-Git "add -A"
  & git commit -m "Apply patch from Codex" | Out-Null
  if ($LASTEXITCODE -ne 0) {
    throw "[GIT] Commit failed (possibly empty patch)."
  }
  Write-Host "[OK] Applied via git apply"
}

# 10) Sync
& git pull --rebase --autostash origin main | Out-Null
if ($LASTEXITCODE -ne 0) { throw "[GIT] pull --rebase failed." }
Run-Git "push origin HEAD:main"

Write-Host "[DONE] Synced with GitHub."
Remove-Item -Force $tmp
