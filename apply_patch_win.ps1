# apply_patch_win.ps1
# Apply patch from clipboard, commit and push to origin/main (Windows).
# Robust to Markdown fences and invisible leading chars (BOM/ZWSP/LRM/RLM/NBSP).
# Supports git-format-patch (git am) and unified diff (git apply).

$ErrorActionPreference = "Stop"

# Ensure we are in a git repo
git rev-parse --is-inside-work-tree | Out-Null

# 1) Read clipboard
$raw = Get-Clipboard
if ([string]::IsNullOrWhiteSpace($raw)) { throw "[ERR] Clipboard is empty." }

# 2) Normalize line endings
$raw = $raw -replace "`r`n", "`n" -replace "`r", "`n"

# 3) Strip common invisible/formatting chars at start
#    BOM U+FEFF, ZWSP U+200B, LRM U+200E, RLM U+200F, NBSP U+00A0
$raw = $raw -replace "^[\uFEFF\u200B\u200E\u200F\u00A0\s]+", ""

# 4) Drop Markdown code fences if present
$raw = $raw -replace "(?s)^\s*```[a-zA-Z0-9_-]*\s*", ""
$raw = $raw -replace "(?s)\s*```\s*$", ""
$raw = $raw.Trim()

# 5) Keep from first 'From <sha>' or 'diff --git' (on any line; allow leading spaces)
$fromMatch = [regex]::Match($raw, "(?m)^\s*From [0-9a-f]{40}\b")
$diffMatch = [regex]::Match($raw, "(?m)^\s*diff --git\s")
if ($fromMatch.Success) {
  $raw = $raw.Substring($fromMatch.Index)
} elseif ($diffMatch.Success) {
  $raw = $raw.Substring($diffMatch.Index)
} else {
  # 5b) Best-effort: synthesize 'diff --git' if we see +++/--- pair
  $plusMatch  = [regex]::Match($raw, "(?m)^\s*\+\+\+\s+b/([^\r\n]+)")
  $minusMatch = [regex]::Match($raw, "(?m)^\s*---\s+(?:a/|/dev/null)([^\r\n]*)")
  if ($plusMatch.Success) {
    $path = $plusMatch.Groups[1].Value.Trim()
    $header = "diff --git a/$path b/$path`n"
    $raw = $header + $raw
  }
}

# 6) Save to temp (UTF-8 no BOM)
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[System.IO.File]::WriteAllText($tmp, $raw, [Text.UTF8Encoding]::new($false))
Write-Host "[i] Patch saved to $tmp"

# 7) Quick diagnostics (first 2 lines + first 16 bytes hex)
$firstLines = ($raw -split "`n")[0..([Math]::Min(1, ($raw -split "`n").Length-1))] -join "`n" 2>$null
if ($firstLines) { Write-Host "[i] First lines:`n$firstLines" }
try {
  $bytes = [Text.Encoding]::UTF8.GetBytes($raw)
  $hex = ($bytes[0..([Math]::Min(15, $bytes.Length-1))] | ForEach-Object { $_.ToString("X2") }) -join " "
  Write-Host "[i] First bytes (hex): $hex"
} catch {}

# 8) Detect formats
$looksGitFormat = ($raw -match "(?m)^\s*From [0-9a-f]{40}\b" -and $raw -match "(?m)^\s*Subject:")
$looksUnified   = ($raw -match "(?m)^\s*diff --git\s")

if (-not ($looksGitFormat -or $looksUnified)) {
  throw "[ERR] Clipboard still does not contain a git-compatible patch. Please copy a unified diff (with 'diff --git') or a git-format patch."
}

# 9) Apply
try {
  if ($looksGitFormat) {
    Write-Host "[i] Trying: git am -3"
    git am -3 "$tmp"
    Write-Host "[OK] Applied via git am"
  } else {
    Write-Host "[i] Trying: git apply --check/--whitespace=fix"
    git apply --check --whitespace=fix "$tmp" | Out-Null
    git apply --whitespace=fix "$tmp"
    git add -A
    git commit -m "Apply patch from Codex" | Out-Null
    Write-Host "[OK] Applied via git apply"
  }
}
catch {
  if ($looksGitFormat) { git am --abort | Out-Null }
  Write-Host "[i] Falling back to: git apply --reject"
  git apply --reject --whitespace=fix "$tmp"
  git add -A
  git commit -m "Apply patch from Codex (with rejects)" | Out-Null
  Write-Host "[OK] Applied via git apply --reject (check *.rej if any)"
}

# 10) Sync explicitly (avoid pull strategy prompt)
git pull --rebase --autostash origin main | Out-Null
git push origin HEAD:main

Write-Host "[DONE] Synced with GitHub."
Remove-Item -Force $tmp
