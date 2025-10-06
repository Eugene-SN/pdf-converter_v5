# apply_patch_win.ps1
# Apply patch from clipboard, commit and push to origin/main (Windows).
# Поддерживает: чистый unified diff или тело heredoc из "Copy as git apply".
# Если в буфере всё в одну строку — сохраняем как PATCH.clipboard.txt и выходим с понятной ошибкой.

$ErrorActionPreference = "Stop"
$gitExe = (Get-Command git -ErrorAction Stop).Source

function Die($msg) { throw $msg }

# 0) Проверка, что мы в репозитории
& $gitExe rev-parse --is-inside-work-tree | Out-Null
if ($LASTEXITCODE -ne 0) { Die "Not a git repository." }

# 1) Буфер
$raw = Get-Clipboard
if ([string]::IsNullOrWhiteSpace($raw)) { Die "[ERR] Clipboard is empty." }

# 2) Нормализация
$raw = $raw -replace "`r`n","`n" -replace "`r","`n"
$raw = $raw -replace "[\uFEFF\u200B\u200E\u200F\u00A0]",""
$raw = $raw.Trim()

# 3) Убираем Markdown-кодовые ограждения ```...```
$raw = $raw -replace "(?s)^\s*```[a-zA-Z0-9_-]*\s*",""
$raw = $raw -replace "(?s)\s*```\s*$",""
$raw = $raw.Trim()

# 4) Если это "Copy as git apply" с heredoc — вырежем тело между <<'EOF' и отдельной строкой EOF
$body = $raw
# простой и надёжный regex для heredoc (многострочный, жадность ограничена)
$match = [regex]::Match($body, "(?ms)<<'EOF'\s*\n(.*?)\nEOF\s*$")
if ($match.Success) {
  $body = $match.Groups[1].Value.Trim()
}

# 5) Должны быть реальные переводы строк
$lineCount = ($body -split "`n").Count
if ($lineCount -le 1) {
  Set-Content -Encoding utf8 -NoNewline -Path "PATCH.clipboard.txt" -Value $body
  Die "[ERR] Clipboard looks like a single line (no real newlines). Saved to PATCH.clipboard.txt. Please recopy as plain text or save to a *.patch file."
}

# 6) Отрезаем возможный префикс до первого diff/From
$fromMatch = [regex]::Match($body, "(?m)^\s*From [0-9a-f]{40}\b")
$diffMatch = [regex]::Match($body, "(?m)^\s*diff --git\s")
if ($fromMatch.Success) {
  $body = $body.Substring($fromMatch.Index)
} elseif ($diffMatch.Success) {
  $body = $body.Substring($diffMatch.Index)
}

# 7) Сохраняем во временный файл
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[IO.File]::WriteAllText($tmp, $body, [Text.UTF8Encoding]::new($false))
Write-Host "[i] Patch saved to $tmp"

# 8) Валидация структуры (минимум: есть diff --git и хотя бы один хунк @@)
$hasDiff = Select-String -Path $tmp -Pattern '^\s*diff --git ' -SimpleMatch -CaseSensitive -List
if (-not $hasDiff) { Die "[ERR] No 'diff --git' headers found. Ask Codex for a unified diff or use 'Copy as git apply'." }

$hasHunk = Select-String -Path $tmp -Pattern '^\s*@@ ' -SimpleMatch -CaseSensitive -List
if (-not $hasHunk) {
  $preview = (Get-Content -Path $tmp -TotalCount 30) -join "`n"
  Die "[ERR] No hunks ('@@ ... @@') found. The patch may be incomplete.`n--- Preview ---`n$preview"
}

# 9) Применение
$looksGitFormat =
  (Select-String -Path $tmp -Pattern '^\s*From [0-9a-f]{40}\b' -CaseSensitive -List) -and
  (Select-String -Path $tmp -Pattern '^\s*Subject:'            -CaseSensitive -List)

if ($looksGitFormat) {
  Write-Host "[i] Trying: git am -3"
  & $gitExe am -3 "$tmp"
  if ($LASTEXITCODE -ne 0) {
    & $gitExe am --abort | Out-Null
    Die "[ERR] git am failed. Export the patch as unified diff instead of git-format."
  }
  Write-Host "[OK] Applied via git am"
} else {
  Write-Host "[i] Trying: git apply --check/--whitespace=fix"
  & $gitExe apply --check --whitespace=fix "$tmp"
  if ($LASTEXITCODE -ne 0) { Die "[ERR] git apply --check failed. Ensure the patch is a plain unified diff." }

  & $gitExe apply --whitespace=fix "$tmp"
  if ($LASTEXITCODE -ne 0) { Die "[ERR] git apply failed." }

  & $gitExe add -A
  if ($LASTEXITCODE -ne 0) { Die "[ERR] git add failed." }

  & $gitExe commit -m "Apply patch from Codex"
  if ($LASTEXITCODE -ne 0) { Die "[ERR] git commit failed (possibly empty patch)." }

  Write-Host "[OK] Applied via git apply"
}

# 10) Синхронизация
& $gitExe pull --rebase --autostash origin main | Out-Null
if ($LASTEXITCODE -ne 0) { Die "[ERR] git pull --rebase failed." }

& $gitExe push origin HEAD:main
if ($LASTEXITCODE -ne 0) { Die "[ERR] git push failed." }

Write-Host "[DONE] Synced with GitHub."
