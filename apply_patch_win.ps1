# apply_patch_win.ps1
# Применяет патч из буфера (Codex) и пушит в GitHub (Windows)

$ErrorActionPreference = "Stop"

# 0) Проверка: мы в git-репозитории
git rev-parse --is-inside-work-tree | Out-Null

# 1) Буфер → строка + нормализация CRLF
$raw = Get-Clipboard
if ([string]::IsNullOrWhiteSpace($raw)) {
  Write-Error "[ERR] Clipboard is empty."
}
$raw = $raw -replace "`r`n", "`n" -replace "`r", "`n"

# 2) Временный файл
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[System.IO.File]::WriteAllText($tmp, $raw, [Text.UTF8Encoding]::new($false))
Write-Host "[i] Patch saved to $tmp"

# 3) Валидация: это diff?
if ($raw -notmatch '^diff --git ') {
  throw "[ERR] В буфере нет заголовков 'diff --git'. Попросите Codex выдать unified diff или 'Copy as git apply'."
}

# 4) Детект git-format patch (git am) или unified diff
$looksGitFormat = ($raw -match '^From [0-9a-f]{40} ' -and $raw -match '(?m)^Subject:')

try {
  if ($looksGitFormat) {
    Write-Host "[i] Trying git am -3…"
    git am -3 "$tmp"
    Write-Host "[OK] Applied via git am"
  }
  else {
    Write-Host "[i] Trying git apply…"
    # Сначала «dry-run» проверка
    git apply --check --whitespace=fix "$tmp" | Out-Null
    git apply --whitespace=fix "$tmp"
    git add -A
    git commit -m "Apply patch from Codex" | Out-Null
    Write-Host "[OK] Applied via git apply"
  }
}
catch {
  # fallback: если не прошёл ни am, ни apply --check — пробуем --reject
  if ($looksGitFormat) {
    Write-Host "[i] git am failed, aborting…"
    git am --abort | Out-Null
  }
  Write-Host "[i] Falling back to: git apply --reject"
  git apply --reject --whitespace=fix "$tmp"
  git add -A
  git commit -m "Apply patch from Codex (with rejects)" | Out-Null
  Write-Host "[OK] Applied via git apply --reject (check *.rej if any)"
}

# 5) Синхронизация
git pull --rebase --autostash | Out-Null
git push
Write-Host "[DONE] Synced with GitHub."

Remove-Item -Force $tmp
