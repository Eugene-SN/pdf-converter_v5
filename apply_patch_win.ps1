# apply_patch_win.ps1
# Apply patch from clipboard, commit and push to origin/main (Windows).
# Без «рефлоу»: берём либо чистый unified diff, либо тело heredoc из "Copy as git apply".
# Если в буфере одна строка/плохое форматирование — сохраняем в PATCH.clipboard.txt и выходим с понятной ошибкой.

$ErrorActionPreference = "Stop"
$gitExe = (Get-Command git -ErrorAction Stop).Source

function Ensure-GitOk {
  & $gitExe rev-parse --is-inside-work-tree | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "Not a git repository." }
}
function Die($msg) { throw $msg }

Ensure-GitOk

# 1) Clipboard
$raw = Get-Clipboard
if ([string]::IsNullOrWhiteSpace($raw)) { Die "[ERR] Clipboard is empty." }

# 2) Normalize and basic clean
$raw = $raw -replace "`r`n","`n" -replace "`r","`n"
$raw = $raw -replace "[\uFEFF\u200B\u200E\u200F\u00A0]",""
$raw = $raw.Trim()

# 3) Drop Markdown fences ```...```
$raw = $raw -replace "(?s)^\s*```[a-zA-Z0-9_-]*\s*",""
$raw = $raw -replace "(?s)\s*```\s*$",""
$raw = $raw.Trim()

# 4) Если это "Copy as git apply" с heredoc — вырежем тело между <<'EOF' и последним отдельным EOF
#   Требуем реальные переводы строк. Если их нет — не пытаемся «умничать».
$body = $raw
if ($body -match "(?s)<<'EOF'") {
  # пробуем отрезать по настоящим переводам
  if ($body -split "`n" | Measure-Object | Select-Object -ExpandProperty Count -gt 2) {
    $m = [regex]::Match($body, "(?ms)<<'EOF'\s*(.*?)\s*^EOF\s*$")
    if ($m.Success) { $body = $m.Groups[1].Value.Trim() }
  }
}

# 5) Должны быть переносы строк; иначе — сохранить для анализа и выйти
if (($body -split "`n").Length -le 1) {
  Set-Content -Encoding utf8 -NoNewline -Path "PATCH.clipboard.txt" -Value $body
  Die "[ERR] Clipboard looks like a single line (no real newlines). Saved to PATCH.clipboard.txt. Please recopy as plain text or save to a *.patch file."
}

# 6) Обрезаем до первого diff/From (если до них есть префикс)
if ($body -match "(?m)^\s*From [0-9a-f]{40}\b") {
  $idx = ([regex]::Match($body,"(?m)^\s*From [0-9a-f]{40}\b")).Index
  $body = $body.Substring($idx)
} elseif ($body -match "(?m)^\s*diff --git\s") {
  $idx = ([regex]::Match($body,"(?m)^\s*diff --git\s")).Index
  $body = $body.Substring($idx)
}

# 7) Сохраняем во временный файл
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[IO.File]::WriteAllText($tmp, $body, [Text.UTF8Encoding]::new($false))
Write-Host "[i] Patch saved to $tmp"

# 8) Строгая валидация структуры (без попыток «чинить»)
if (-not (Select-String -Path $tmp -Pattern '^\s*diff --git ' -SimpleMatch -CaseSensitive -List)) {
  Die "[ERR] No 'diff --git' headers found. Ask Codex for a unified diff or use 'Copy as git apply'."
}
if (-not (Select-String -Path $tmp -Pattern '^\s*@@ ' -SimpleMatch -CaseSensitive -List)) {
  $preview = (Get-Content -Raw -Path $tmp | Select-Object -First 1)
  Die "[ERR] No hunks ('@@ ... @@') found. The patch may be incomplete."
}

# 9) Применение
$looksGitFormat = (Select-String -Path $tmp -Pattern '^\s*From [0-9a-f]{40}\b' -CaseSensitive -List) -and
                  (Select-String -Path $tmp -Pattern '^\s*Subject:' -CaseSensitive -List)

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

  & $gitExe apply --whitespace=fix "$tmp"; if ($LASTEXITCODE -ne 0) { Die "[ERR] git apply failed." }
  & $gitExe add -A; if ($LASTEXITCODE -ne 0) { Die "[ERR] git add failed." }

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
