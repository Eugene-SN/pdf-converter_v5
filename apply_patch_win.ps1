# apply_patch_win.ps1
# Применяет патч из буфера (Codex heredoc или чистый diff), коммитит и пушит в origin/main.

$ErrorActionPreference = "Stop"

# Git
$git = (Get-Command git -ErrorAction Stop).Source

function Die($msg) { throw $msg }

# Консоль/вывод → UTF-8 (читаемые логи и превью)
try { chcp 65001 | Out-Null } catch {}
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
[Console]::InputEncoding  = [System.Text.UTF8Encoding]::new()
$OutputEncoding           = [System.Text.UTF8Encoding]::new()

# 0) Проверка repo
& $git rev-parse --is-inside-work-tree | Out-Null
if ($LASTEXITCODE -ne 0) { Die "Not a git repository." }

# 1) Буфер «как есть»
$raw = Get-Clipboard -Raw
if ([string]::IsNullOrWhiteSpace($raw)) { Die "[ERR] Clipboard is empty." }

# 2) Нормализация переводов строк/невидимых символов
$raw = $raw -replace "`r`n","`n" -replace "`r","`n"
$raw = $raw -replace "[\uFEFF\u200B\u200E\u200F\u00A0]",""
$raw = $raw.Trim()

# 3) Снять ```fences``` если есть
$raw = $raw -replace "(?s)^\s*```[a-zA-Z0-9_-]*\s*",""
$raw = $raw -replace "(?s)\s*```\s*$",""
$raw = $raw.Trim()

# 4) Если явно одна строка — сохраним и подскажем
if ((($raw -split "`n").Count) -le 1) {
  Set-Content -Encoding utf8 -NoNewline -Path "PATCH.clipboard.txt" -Value $raw
  Die "[ERR] Clipboard looks like a single line. Saved to PATCH.clipboard.txt. Recopy as plain text or save to *.patch."
}

# === ПАРСИНГ КАНДИДАТОВ ===

function Extract-HeredocBody([string]$text) {
  # ищем строку с <<'EOF' / <<EOF / << "EOF"
  $lines = $text -split "`n"
  $start = $null; $end = $null
  for ($i=0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match "^[\s\S]*<<\s*(['""]?)EOF\1\s*$") { $start = $i + 1; break }
  }
  if ($start -ne $null) {
    for ($j=$start; $j -lt $lines.Count; $j++) {
      if ($lines[$j] -match "^\s*EOF\s*$") { $end = $j; break }
    }
  }
  if ($start -ne $null -and $end -ne $null -and $end -gt $start) {
    return (($lines[$start..($end-1)] -join "`n").Trim())
  }
  return $null
}

function Extract-FromFirstDiff([string]$text) {
  $m = [regex]::Match($text, "(?m)^\s*diff --git\s")
  if ($m.Success) { return $text.Substring($m.Index).Trim() }
  return $null
}

function Score-Patch([string]$body) {
  if ([string]::IsNullOrWhiteSpace($body)) { return 0 }
  $diffs = ([regex]::Matches($body, "(?m)^\s*diff --git\s")).Count
  $hunks = ([regex]::Matches($body, "(?m)^\s*@@ ")).Count
  return ($diffs*1000 + $hunks)  # diff важнее
}

# кандидаты: (1) heredoc, (2) с первого diff из raw, (3) fallback: с первого diff из извлечённого heredoc (если он вдруг был пустоват)
$body1 = Extract-HeredocBody $raw
$body2 = Extract-FromFirstDiff $raw
$body3 = $null
if ($body1) { $body3 = Extract-FromFirstDiff $body1 }

# выберем лучший по количеству diff/hunk
$candidates = @($body1, $body2, $body3) | Where-Object { $_ -ne $null }
if ($candidates.Count -eq 0) {
  Set-Content -Encoding utf8 -Path "PATCH.clipboard.txt" -Value $raw
  Set-Content -Encoding utf8 -Path "PATCH.preview.txt" -Value (($raw -split "`n" | Select-Object -First 80) -join "`n")
  Die "[ERR] No 'diff --git' candidates found. Saved PATCH.clipboard.txt / PATCH.preview.txt."
}

$best = $candidates | Sort-Object { - (Score-Patch $_) } | Select-Object -First 1

# Сдвигаем к первому From/diff внутри лучшего
$fromM = [regex]::Match($best, "(?m)^\s*From [0-9a-f]{40}\b")
$diffM = [regex]::Match($best, "(?m)^\s*diff --git\s")
if ($fromM.Success) { $best = $best.Substring($fromM.Index) }
elseif ($diffM.Success) { $best = $best.Substring($diffM.Index) }
$best = $best.Trim()

# Быстрая проверка структуры
if (([regex]::Matches($best, "(?m)^\s*diff --git\s")).Count -eq 0) {
  Set-Content -Encoding utf8 -Path "PATCH.preview.txt" -Value (($best -split "`n" | Select-Object -First 80) -join "`n")
  Die "[ERR] No 'diff --git' headers found after extraction. Preview saved to PATCH.preview.txt."
}
if (([regex]::Matches($best, "(?m)^\s*@@ ")).Count -eq 0) {
  Set-Content -Encoding utf8 -Path "PATCH.preview.txt" -Value (($best -split "`n" | Select-Object -First 80) -join "`n")
  Die "[ERR] No hunks ('@@ ... @@') found. Preview saved to PATCH.preview.txt."
}

# 5) Сохраняем во временный *.patch (чистый UTF-8 без BOM)
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[IO.File]::WriteAllText($tmp, $best, [Text.UTF8Encoding]::new($false))
Write-Host "[i] Patch saved to $tmp"

# 6) Применение (мягкая эскалация)
# Сначала пробуем 3-way (часто помогает при дрейфе контекста), затем обычный, затем --reject как крайний случай
Write-Host "[i] Trying: git apply --check --3way"
& $git apply --check --3way "$tmp"
if ($LASTEXITCODE -eq 0) {
  & $git apply --3way "$tmp"
} else {
  Write-Host "[i] Fallback: git apply --check (no 3way)"
  & $git apply --check --whitespace=fix "$tmp"
  if ($LASTEXITCODE -eq 0) {
    & $git apply --whitespace=fix "$tmp"
  } else {
    Write-Host "[i] Fallback: git apply --reject"
    & $git apply --reject --whitespace=fix "$tmp"
    if ($LASTEXITCODE -ne 0) {
      Set-Content -Encoding utf8 -Path "PATCH.preview.txt" -Value ((Get-Content -Path $tmp -TotalCount 120) -join "`n")
      Die "[ERR] git apply --reject failed. Preview saved to PATCH.preview.txt."
    }
  }
}

# 7) Коммит и пуш
& $git add -A
& $git commit -m "Apply patch from Codex" | Out-Null
# если нечего коммитить (пустой патч) — commit вернёт код ≠0, не считаем это критом
if ($LASTEXITCODE -ne 0) { Write-Host "[i] Nothing to commit (possibly already applied)." }

# Обновимся и запушим
& $git pull --rebase --autostash origin main | Out-Null
& $git push origin HEAD:main

Write-Host "[DONE] Synced with GitHub."
