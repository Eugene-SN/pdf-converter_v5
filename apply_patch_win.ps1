# apply_patch_win.ps1
$ErrorActionPreference = "Stop"

# Resolve exact git executable
$gitExe = (Get-Command git -ErrorAction Stop).Source

function Run-Git {
  param([string[]] $GitArgs, [switch] $Quiet)
  if (!$GitArgs -or $GitArgs.Count -eq 0) { throw "[GIT] Empty argument list" }
  if ($Quiet) { & $gitExe @GitArgs | Out-Null } else { & $gitExe @GitArgs }
  if ($LASTEXITCODE -ne 0) { throw "[GIT] Command failed: git $($GitArgs -join ' ')" }
}

# 0) Ensure repo
Run-Git @('rev-parse','--is-inside-work-tree') -Quiet

# 1) Clipboard
$raw = Get-Clipboard
if ([string]::IsNullOrWhiteSpace($raw)) { throw "[ERR] Clipboard is empty." }

# 2) Normalize newlines + strip invisible chars
$raw = $raw -replace "`r`n","`n" -replace "`r","`n"
$raw = $raw -replace "[\uFEFF\u200B\u200E\u200F\u00A0]",""
$raw = $raw.Trim()

# 3) Strip Markdown fences
$raw = $raw -replace "(?s)^\s*```[a-zA-Z0-9_-]*\s*", ""
$raw = $raw -replace "(?s)\s*```\s*$", ""
$raw = $raw.Trim()

# 4) Also strip invisible chars at start of EACH line (не трогаем обычные пробелы)
$raw = [regex]::Replace($raw, "(?m)^[\uFEFF\u200B\u200E\u200F\u00A0]+", "")

# 5) If looks single-line -> heuristic reflow
if ( ($raw -split "`n").Length -le 2 ) {
  Write-Host "[i] Heuristic reflow: input looks like a single line, trying to insert newlines…"
  $s = " " + ($raw -replace "\s+"," ").Trim()

  $tokens = @(
    ' diff --git ',
    ' new file mode ',
    ' deleted file mode ',
    ' index ',
    ' --- ',
    ' +++ ',
    ' @@ '
  )
  foreach ($t in $tokens) {
    $s = $s -replace [regex]::Escape($t), ("`n" + $t.Trim())
  }

  # добавим переводы строк перед строками с добавлениями контента, если они слиплись
  $s = $s -replace "(?<!`n)\+([^\+\-@ ].*)", "`n+$1"

  # cleanup
  $s = $s -replace "`n{2,}","`n"
  $raw = $s.Trim()
}

# 6) Keep from first 'From <sha>' or 'diff --git'
$from = [regex]::Match($raw, "(?m)^\s*From [0-9a-f]{40}\b")
$diff = [regex]::Match($raw, "(?m)^\s*diff --git\s+a/([^\s]+)\s+b/([^\s]+)")
if     ($from.Success) { $raw = $raw.Substring($from.Index) }
elseif ($diff.Success) { $raw = $raw.Substring($diff.Index) }

# 6b) Ensure we have both --- and +++ headers for each file block
# простая коррекция первой секции: если есть '--- /dev/null' и нет последующего '+++ b/…', добавим из diff-заголовка
if ($diff.Success) {
  $bPath = $diff.Groups[2].Value
  # если первая встречная линия --- /dev/null, а следующей +++ нет — вставим
  $lines = $raw -split "`n"
  for ($i=0; $i -lt [Math]::Min($lines.Length, 20); $i++) {
    if ($lines[$i] -match "^\s*---\s+/dev/null\s*$") {
      $hasPlus = $false
      for ($j=$i+1; $j -lt [Math]::Min($lines.Length, $i+5); $j++) {
        if ($lines[$j] -match "^\s*\+\+\+\s+b/") { $hasPlus = $true; break }
        if ($lines[$j] -match "^\s*@@ ") { break }
      }
      if (-not $hasPlus) {
        $insertAt = $i+1
        $lines = $lines[0..($insertAt-1)] + @("+++ b/$bPath") + $lines[$insertAt..($lines.Length-1)]
        $raw = ($lines -join "`n")
      }
      break
    }
  }
}

# 7) Require at least one hunk '@@'
if ($raw -notmatch "(?m)^\s*@@") {
  $preview = (($raw -split "`n") | Select-Object -First 30) -join "`n"
  throw "[ERR] No hunks found (^\s*@@). The clipboard likely contains headers only or was mangled by formatting.`n--- Preview ---`n$preview"
}

# 8) Save temp (UTF-8 no BOM)
$tmp = Join-Path $env:TEMP ("codex_patch_{0}.patch" -f ([guid]::NewGuid().ToString("N")))
[IO.File]::WriteAllText($tmp,$raw,[Text.UTF8Encoding]::new($false))
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
    Run-Git @('am','-3',"$tmp")
    Write-Host "[OK] Applied via git am"
  } else {
    Write-Host "[i] Trying: git apply --check/--whitespace=fix"
    & $gitExe apply --check --whitespace=fix "$tmp"
    if ($LASTEXITCODE -ne 0) {
      Write-Host "[i] git apply --check failed, verbose output:"
      & $gitExe apply -v --check --whitespace=fix "$tmp"
      throw "[GIT] git apply --check failed"
    }
    Run-Git @('apply','--whitespace=fix',"$tmp")
    Run-Git @('add','-A')
    & $gitExe commit -m "Apply patch from Codex" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "[GIT] Commit failed (possibly empty patch)." }
    Write-Host "[OK] Applied via git apply"
  }
}
catch {
  if ($gitFormat) { & $gitExe am --abort | Out-Null }
  Write-Host "[i] Falling back to: git apply --reject"
  & $gitExe apply --reject --whitespace=fix "$tmp"
  if ($LASTEXITCODE -ne 0) {
    Write-Host "[i] git apply --reject failed, verbose output:"
    & $gitExe apply -v --reject --whitespace=fix "$tmp"
    throw "[GIT] git apply --reject failed"
  }
  Run-Git @('add','-A')
  & $gitExe commit -m "Apply patch from Codex (with rejects)" | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "[GIT] Commit failed after rejects." }
  Write-Host "[OK] Applied via git apply --reject (check *.rej if any)"
}

# 12) Sync
& $gitExe pull --rebase --autostash origin main | Out-Null
if ($LASTEXITCODE -ne 0) { throw "[GIT] pull --rebase failed." }
Run-Git @('push','origin','HEAD:main')

Write-Host "[DONE] Synced with GitHub."
Remove-Item -Force $tmp
