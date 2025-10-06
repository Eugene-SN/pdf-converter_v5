# apply_patch_win.ps1
# Apply patch from clipboard, commit and push to origin/main (Windows).
# Robust to Markdown fences and invisible leading chars (BOM/ZWSP/LRM/RLM/NBSP),
# including those appearing at the start of EACH LINE.
# Supports git-format-patch (git am) and unified diff (git apply).

$ErrorActionPreference = "Stop"

function Run-Git([string]$ArgsLine, [switch]$Quiet) {
  if ($Quiet) { & git $ArgsLine | Out-Null } else { & git $ArgsLine }
  if ($LASTEXITCODE -ne 0) { throw "[GIT] Command failed: git $ArgsLine" }
}

# 0) Ensure repo
Run-Git "rev-parse --is-inside-work-tree" -Quiet

# 1) Clipboard
$raw = Get-Clipboard
if ([string]::IsNullOrWhiteSpace($raw)) { throw "[ERR] Clipboard is empty." }

# 2) Normalize newlines
$raw = $raw -replace "`r`n","`n" -replace "`r","`n"

# 3) Remove common invisible chars at the very beginning of the WHOLE text
$raw = $raw -replace "^[`uFEFF`u200B`u200E`u200F`u00A0]+", ""

# 4) Strip Markdown code fences if present
$raw = $raw -replace "(?s)^\s*```[a-zA-Z0-9_-]*\s*", ""
$raw = $raw -replace "(?s)\s*```\s*$", ""
$raw = $raw.Trim()

# 5) Remove invisible chars at the START OF EACH LINE (but DO NOT remove normal spaces)
#    This fixes cases where @@ lines or diff headers are prefixed with ZWSP/LRM etc.
$raw = [regex]::Replace($raw, "(?m)^[`uFEFF`u200B`u200E`u200F`u00A0]+", "")

# 6) Keep from first 'From <sha>' or 'diff --git'
$from = [regex]::Match($raw, "(?m)^\s*From [0-9a-f]{40}\b")
$diff = [regex]::Match($raw, "(?m)^\s*diff --git\s")
if     ($from.Success) { $raw = $raw.Substring($from.Index) }
elseif ($diff.Success) { $raw = $raw.Substring($diff.Index) }
else {
  # Try to synthesize a 'diff --git' header from +++/--- lines (best-effort).
  $plus = [regex]::Match($raw, "(?m)^\s*\+\+\+\s+b/([^\r\n]+)")
  if ($plus.Success) {
    $path = $plus.Groups[1].Value.Trim()
    $raw  = "diff --git a/$path b/$path`n$raw"
  }
}

# 7) Must have at least one hunk @@ (allow leading invisible/space)
if ($raw -notmatch "(?m)^\s*@@") {
  # Диагностика: покажем первые 20 строк для наглядности
  $preview = (($raw -split "`n") | Select-Object -First 20) -join "`n"
  throw "[ERR] No hunks found (^\s*@@). The clipboard likely contains headers only or was mangled by formatting.`n--- Preview ---`n$preview"
}

# 8) Save to temp (UTF-8 no BOM)
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[IO.File]::WriteAllText($tmp, $raw, [Text.UTF8Encoding]::new($false))
Write-Host "[i] Patch saved to $tmp"

# 9) Diagnostics
$lines = $raw -split "`n"
$first4 = ($lines | Select-Object -First 4) -join "`n"
$firstHunk = ($lines | Where-Object { $_ -match "^\s*@@" } | Select-Object -First 1)
Write-Host "[i] First lines:`n$first4"
if ($firstHunk) { Write-Host "[i] First hunk line: $firstHunk" }

# 10) Detect format
$gitFormat = ($raw -match "(?m)^\s*From [0-9a-f]{40}\b" -and $raw -match "(?m)^\s*Subject:")
$unified   = ($raw -match "(?m)^\s*diff --git\s")

# 11) Apply
try {
  if ($gitFormat) {
    Write-Host "[i] Trying: git am -3"
    Run-Git "am -3 `"$tmp`""
    Write-Host "[OK] Applied via git am"
  } else {
    Write-Host "[i] Trying: git apply --check/--whitespace=fix"
    & git apply --check --whitespace=fix "$tmp"
    if ($LASTEXITCODE -ne 0) {
      Write-Host "[i] git apply --check failed, verbose output:"
      & git apply -v --check --whitespace=fix "$tmp"
      throw "[GIT] git apply --check failed"
    }
    Run-Git "apply --whitespace=fix `"$tmp`""
    Run-Git "add -A"
    & git commit -m "Apply patch from Codex" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "[GIT] Commit failed (possibly empty patch)." }
    Write-Host "[OK] Applied via git apply"
  }
}
catch {
  if ($gitFormat) { & git am --abort | Out-Null }
  Write-Host "[i] Falling back to: git apply --reject"
  & git apply --reject --whitespace=fix "$tmp"
  if ($LASTEXITCODE -ne 0) {
    Write-Host "[i] git apply --reject failed, verbose output:"
    & git apply -v --reject --whitespace=fix "$tmp"
    throw "[GIT] git apply --reject failed"
  }
  Run-Git "add -A"
  & git commit -m "Apply patch from Codex (with rejects)" | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "[GIT] Commit failed after rejects." }
  Write-Host "[OK] Applied via git apply --reject (check *.rej if any)"
}

# 12) Sync explicitly
& git pull --rebase --autostash origin main | Out-Null
if ($LASTEXITCODE -ne 0) { throw "[GIT] pull --rebase failed." }
Run-Git "push origin HEAD:main"

Write-Host "[DONE] Synced with GitHub."
Remove-Item -Force $tmp
