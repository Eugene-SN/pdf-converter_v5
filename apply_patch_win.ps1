# apply_patch_win.ps1
# Применяет патч из буфера (Codex heredoc или чистый unified diff), авто-чинит типовые кривизны и пушит в origin/main.

$ErrorActionPreference = "Stop"
function Die($msg) { throw $msg }

# Git
$git = (Get-Command git -ErrorAction Stop).Source

# Кодировки/вывод
try { chcp 65001 | Out-Null } catch {}
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
[Console]::InputEncoding  = [System.Text.UTF8Encoding]::new()
$OutputEncoding           = [System.Text.UTF8Encoding]::new()

# --- helpers ---
function Save-Utf8NoBom([string]$Path, [string]$Text) {
  [System.IO.File]::WriteAllText($Path, $Text, [System.Text.UTF8Encoding]::new($false))
}
function Read-ClipboardRaw {
  $raw = Get-Clipboard -Raw
  if ([string]::IsNullOrWhiteSpace($raw)) { Die "[ERR] Clipboard is empty." }
  $raw = $raw -replace "`r`n","`n" -replace "`r","`n"
  $raw = $raw -replace "[\uFEFF\u200B\u200E\u200F\u00A0]",""
  $raw.Trim()
}
function Extract-HeredocBody([string]$text) {
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
  return ($diffs*1000 + $hunks)
}
function Sanitize-Patch([string]$body) {
  $lines = $body -split "`n"

  # 1) Исправить a//dev/null → a/dev/null и --- a//dev/null → --- /dev/null
  for ($i=0; $i -lt $lines.Count; $i++) {
    $ln = $lines[$i]
    if ($ln -match "^(diff --git)\s+a//dev/null\s+b/") {
      $lines[$i] = $ln -replace "a//dev/null","a/dev/null"
    }
    if ($ln -match "^\-\-\-\s+a//dev/null\s*$") {
      $lines[$i] = "--- /dev/null"
    }
    if ($ln -match "^\-\-\-\s+a/dev/null\s*$") {
      $lines[$i] = "--- /dev/null"
    }
  }

  # 2) Для новых файлов — если нет new file mode 100644, вставим после заголовка diff
  for ($i=0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match "^(diff --git)\s+a/(?:dev/null|[^ ]+)\s+b/([^ ]+)\s*$") {
      $j = $i + 1
      $hasNewFileMode = $false; $hasIndex = $false
      while ($j -lt $lines.Count -and $lines[$j] -notmatch "^\s*diff --git\s") {
        if ($lines[$j] -match "^new file mode\s+\d+") { $hasNewFileMode = $true }
        if ($lines[$j] -match "^index\s+[0-9a-f]{7,}\.\.[0-9a-f]{7,}(\s+\d+)?$") { $hasIndex = $true }
        if ($lines[$j] -match "^\@\@ ") { break }
        $j++
      }
      if (-not $hasNewFileMode) {
        $lines = $lines[0..$i] + @("new file mode 100644") + $lines[($i+1)..($lines.Count-1)]
      }
      # 3) Убедимся, что index ... заканчивается на режим (100644)
      #    (у новых файлов часто нет, git apply иногда ругается)
      for ($k=$i+1; $k -lt [Math]::Min($lines.Count, $i+8); $k++) {
        if ($lines[$k] -match "^index\s+([0-9a-f]{7,})\.\.([0-9a-f]{7,})(\s+\d+)?$") {
          if (-not $lines[$k].Contains(" 100644")) {
            $lines[$k] = $lines[$k] + " 100644"
          }
          break
        }
      }
    }
  }

  # 4) Гарантируем завершающую пустую строку
  $text = ($lines -join "`n").TrimEnd() + "`n"
  return $text
}
function Try-GitApply([string]$patchPath, [switch]$Use3way, [switch]$Reject) {
  if ($Use3way) {
    & $git apply --check --3way "$patchPath" 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { & $git apply --3way "$patchPath"; return $LASTEXITCODE }
  } elseif ($Reject) {
    & $git apply --reject --whitespace=fix "$patchPath"; return $LASTEXITCODE
  } else {
    & $git apply --check --whitespace=fix "$patchPath" 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { & $git apply --whitespace=fix "$patchPath"; return $LASTEXITCODE }
  }
  return 1
}

# --- проверка репозитория ---
& $git rev-parse --is-inside-work-tree | Out-Null
if ($LASTEXITCODE -ne 0) { Die "Not a git repository." }

# --- читаем буфер и выделяем тело unified diff ---
$raw = Read-ClipboardRaw

$body1 = Extract-HeredocBody $raw
$body2 = Extract-FromFirstDiff $raw
$body3 = $null
if ($body1) { $body3 = Extract-FromFirstDiff $body1 }
$candidates = @($body1, $body2, $body3) | Where-Object { $_ -ne $null }
if ($candidates.Count -eq 0) {
  Save-Utf8NoBom "PATCH.clipboard.txt" $raw
  Save-Utf8NoBom "PATCH.preview.txt" (($raw -split "`n" | Select-Object -First 120) -join "`n")
  Die "[ERR] No 'diff --git' candidates found. Saved PATCH.clipboard.txt / PATCH.preview.txt."
}
$best = $candidates | Sort-Object { - (Score-Patch $_) } | Select-Object -First 1

# сдвиг к первому From/diff
$fromM = [regex]::Match($best, "(?m)^\s*From [0-9a-f]{40}\b")
$diffM = [regex]::Match($best, "(?m)^\s*diff --git\s")
if ($fromM.Success) { $best = $best.Substring($fromM.Index) }
elseif ($diffM.Success) { $best = $best.Substring($diffM.Index) }
$best = $best.Trim()

# сохраняем «как есть»
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
Save-Utf8NoBom $tmp $best
Write-Host "[i] Patch saved to $tmp"

# быстрая структурная проверка
if (([regex]::Matches($best, "(?m)^\s*diff --git\s")).Count -eq 0) {
  Save-Utf8NoBom "PATCH.preview.txt" (($best -split "`n" | Select-Object -First 120) -join "`n")
  Die "[ERR] No 'diff --git' headers found after extraction. Preview saved to PATCH.preview.txt."
}
if (([regex]::Matches($best, "(?m)^\s*@@ ")).Count -eq 0) {
  Save-Utf8NoBom "PATCH.preview.txt" (($best -split "`n" | Select-Object -First 120) -join "`n")
  Die "[ERR] No hunks ('@@ ... @@') found. Preview saved to PATCH.preview.txt."
}

# --- попытка применить «сырой» патч ---
if (Try-GitApply -patchPath $tmp -Use3way) { goto CommitAndPush }
if (Try-GitApply -patchPath $tmp)        { goto CommitAndPush }

# --- санитарная обработка и повтор ---
$san = Sanitize-Patch $best
$sanPath = [System.IO.Path]::ChangeExtension($tmp, ".sanitized.patch")
Save-Utf8NoBom $sanPath $san
Write-Host "[i] Sanitized patch → $sanPath"

if (Try-GitApply -patchPath $sanPath -Use3way) { goto CommitAndPush }
if (Try-GitApply -patchPath $sanPath)          { goto CommitAndPush }
if (Try-GitApply -patchPath $sanPath -Reject)  { goto CommitAndPush }

Save-Utf8NoBom "PATCH.preview.txt" ((Get-Content -LiteralPath $sanPath -TotalCount 200) -join "`n")
Die "[ERR] git apply failed even after sanitizing. Preview saved to PATCH.preview.txt."

:CommitAndPush
& $git add -A
& $git commit -m "Apply patch from Codex" | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "[i] Nothing to commit (possibly already applied)." }

& $git pull --rebase --autostash origin main | Out-Null
& $git push origin HEAD:main
Write-Host "[DONE] Synced with GitHub."
