# apply_patch_win.ps1
# Apply patch from clipboard, commit and push to origin/main (Windows).
# Works with git-format-patch ("From <sha> ... Subject:") and unified diff ("diff --git ...").

$ErrorActionPreference = "Stop"

# 0) Ensure we are inside a git work tree
git rev-parse --is-inside-work-tree | Out-Null

# 1) Read clipboard and normalize newlines
$raw = Get-Clipboard
if ([string]::IsNullOrWhiteSpace($raw)) {
  throw "[ERR] Clipboard is empty."
}
# Normalize CRLF/CR to LF for git apply
$raw = $raw -replace "`r`n", "`n" -replace "`r", "`n"

# 2) Save to a temp file (UTF-8 without BOM)
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[System.IO.File]::WriteAllText($tmp, $raw, [Text.UTF8Encoding]::new($false))
Write-Host "[i] Patch saved to $tmp"

# 3) Validate: must look like a git-compatible diff
if ($raw -notmatch '^diff --git ') {
  throw "[ERR] Clipboard does not contain 'diff --git' headers. Ask Codex for a unified diff or 'Copy as git apply'."
}

# 4) Detect git-format vs unified
$looksGitFormat = ($raw -match '^From [0-9a-f]{40} ' -and $raw -match '(?m)^Subject:')

try {
  if ($looksGitFormat) {
    Write-Host "[i] Trying: git am -3"
    git am -3 "$tmp"
    Write-Host "[OK] Applied via git am"
  }
  else {
    Write-Host "[i] Trying: git apply --check/--whitespace=fix"
    git apply --check --whitespace=fix "$tmp" | Out-Null
    git apply --whitespace=fix "$tmp"
    git add -A
    git commit -m "Apply patch from Codex" | Out-Null
    Write-Host "[OK] Applied via git apply"
  }
}
catch {
  if ($looksGitFormat) {
    git am --abort | Out-Null
  }
  Write-Host "[i] Falling back to: git apply --reject"
  git apply --reject --whitespace=fix "$tmp"
  git add -A
  git commit -m "Apply patch from Codex (with rejects)" | Out-Null
  Write-Host "[OK] Applied via git apply --reject (check *.rej if any)"
}

# 5) Sync with origin/main explicitly (avoid divergent pull behavior)
git pull --rebase --autostash origin main | Out-Null
git push origin HEAD:main

Write-Host "[DONE] Synced with GitHub."
Remove-Item -Force $tmp
