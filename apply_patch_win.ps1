# apply_patch_win.ps1
# Применяет патч из буфера, коммитит и пушит в origin/main (Windows).
# Поддерживает "Copy as git apply" (heredoc) и чистый unified diff.
# При проблемах создаёт PATCH.clipboard.txt (полный сырой ввод) и PATCH.preview.txt (первые 50 строк).

$ErrorActionPreference = "Stop"
$git = (Get-Command git -ErrorAction Stop).Source
function Die($msg) { throw $msg }

# 0) Проверка repo
& $git rev-parse --is-inside-work-tree | Out-Null
if ($LASTEXITCODE -ne 0) { Die "Not a git repository." }

# 1) Читаем буфер
$raw = Get-Clipboard
if ([string]::IsNullOrWhiteSpace($raw)) { Die "[ERR] Clipboard is empty." }

# 2) Нормализация перевода строк и невидимых символов
$raw = $raw -replace "`r`n","`n" -replace "`r","`n"
$raw = $raw -replace "[\uFEFF\u200B\u200E\u200F\u00A0]",""
$raw = $raw.Trim()

# 3) Убираем Markdown-ограждения ```...```
$raw = $raw -replace "(?s)^\s*```[a-zA-Z0-9_-]*\s*",""
$raw = $raw -replace "(?s)\s*```\s*$",""
$raw = $raw.Trim()

# 4) Если всего одна строка — фиксируем и выходим (мессенджер «сплющил»)
if ((($raw -split "`n").Count) -le 1) {
  Set-Content -Encoding utf8 -NoNewline -Path "PATCH.clipboard.txt" -Value $raw
  Die "[ERR] Clipboard looks like a single line (no real newlines). Saved to PATCH.clipboard.txt. Recopy as plain text or save to a *.patch file."
}

# 5) Пытаемся вырезать тело heredoc между <<EOF и EOF (много вариантов)
$lines = $raw -split "`n"
$startIdx = $null; $endIdx = $null
for ($i=0; $i -lt $lines.Count; $i++) {
  if ($lines[$i] -match "^[\s\S]*<<\s*(['""]?)EOF\1\s*$") {
    $startIdx = $i + 1
    break
  }
}
if ($startIdx -ne $null) {
  for ($j=$startIdx; $j -lt $lines.Count; $j++) {
    if ($lines[$j] -match "^\s*EOF\s*$") { $endIdx = $j; break }
  }
}

if ($startIdx -ne $null -and $endIdx -ne $null -and $endIdx -gt $startIdx) {
  $bodyLines = $lines[$startIdx..($endIdx-1)]
  $body = ($bodyLines -join "`n").Trim()
} else {
  # 6) Нет heredoc — берём с первого diff --git до конца
  $firstDiff = $null
  for ($k=0; $k -lt $lines.Count; $k++) {
    if ($lines[$k] -match "^\s*diff --git\s") { $firstDiff = $k; break }
  }
  if ($firstDiff -ne $null) {
    $body = ($lines[$firstDiff..($lines.Count-1)] -join "`n").Trim()
  } else {
    # Ничего не нашли — диагностируем ввод
    Set-Content -Encoding utf8 -Path "PATCH.clipboard.txt" -Value $raw
    $prev = ($lines | Select-Object -First 50) -join "`n"
    Set-Content -Encoding utf8 -Path "PATCH.preview.txt" -Value $prev
    Die "[ERR] No 'diff --git' headers found. Saved raw input to PATCH.clipboard.txt and preview to PATCH.preview.txt. Ensure you copied a unified diff or 'Copy as git apply'."
  }
}

# 7) Контроль: есть несколько строк?
if ((($body -split "`n").Count) -le 1) {
  Set-Content -Encoding utf8 -NoNewline -Path "PATCH.clipboard.txt" -Value $raw
  Die "[ERR] Extracted patch body is single-line. Saved original to PATCH.clipboard.txt. Recopy as plain text."
}

# 8) На всякий случай отрезаем префиксы до первых From/diff
$fromM = [regex]::Match($body, "(?m)^\s*From [0-9a-f]{40}\b")
$diffM = [regex]::Match($body, "(?m)^\s*diff --git\s")
if ($fromM.Success) { $body = $body.Substring($fromM.Index) }
elseif ($diffM.Success) { $body = $body.Substring($diffM.Index) }

# 9) Сохраняем во временный файл
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[IO.File]::WriteAllText($tmp, $body, [Text.UTF8Encoding]::new($false))
Write-Host "[i] Patch saved to $tmp"

# 10) Валидация минимальной структуры
$hasDiff = Select-String -Path $tmp -Pattern '^\s*diff --git ' -SimpleMatch -CaseSensitive -List
if (-not $hasDiff) {
  Set-Content -Encoding utf8 -Path "PATCH.preview.txt" -Value ((Get-Content -Path $tmp -TotalCount 50) -join "`n")
  Die "[ERR] No 'diff --git' headers found after extraction. Preview saved to PATCH.preview.txt."
}
$hasHunk = Select-String -Path $tmp -Pattern '^\s*@@ ' -SimpleMatch -CaseSensitive -List
if (-not $hasHunk) {
  Set-Content -Encoding utf8 -Path "PATCH.preview.txt" -Value ((Get-Content -Path $tmp -TotalCount 50) -join "`n")
  Die "[ERR] No hunks ('@@ ... @@') found. The patch may be incomplete. Preview saved to PATCH.preview.txt."
}

# 11) Определяем формат
$looksGitFormat =
  (Select-String -Path $tmp -Pattern '^\s*From [0-9a-f]{40}\b' -CaseSensitive -List) -and
  (Select-String -Path $tmp -Pattern '^\s*Subject:'            -CaseSensitive -List)

# 12) Применяем
if ($looksGitFormat) {
  Write-Host "[i] Trying: git am -3"
  & $git am -3 "$tmp"
  if ($LASTEXITCODE -ne 0) {
    & $git am --abort | Out-Null
    Die "[ERR] git am failed. Export the patch as unified diff instead of git-format."
  }
  Write-Host "[OK] Applied via git am"
} else {
  Write-Host "[i] Trying: git apply --check/--whitespace=fix"
  & $git apply --check --whitespace=fix "$tmp"
  if ($LASTEXITCODE -ne 0) { Die "[ERR] git apply --check failed. Ensure the patch is a plain unified diff." }

  & $git apply --whitespace=fix "$tmp"
  if ($LASTEXITCODE -ne 0) { Die "[ERR] git apply failed." }

  & $git add -A
  if ($LASTEXITCODE -ne 0) { Die "[ERR] git add failed." }

  & $git commit -m "Apply patch from Codex"
  if ($LASTEXITCODE -ne 0) { Die "[ERR] git commit failed (possibly empty patch)." }

  Write-Host "[OK] Applied via git apply"
}

# 13) Синхронизация
& $git pull --rebase --autostash origin main | Out-Null
if ($LASTEXITCODE -ne 0) { Die "[ERR] git pull --rebase failed." }

& $git push origin HEAD:main
if ($LASTEXITCODE -ne 0) { Die "[ERR] git push failed." }

Write-Host "[DONE] Synced with GitHub."
