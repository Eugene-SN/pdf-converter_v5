# apply_codex_patch.ps1
# Специализированный скрипт для применения Codex патчей в VS Code на Windows
# Версия: 2.0 (исправленная)

param(
    [switch]$DryRun = $false,
    [switch]$AutoCommit = $false,
    [switch]$Verbose = $false
)

# Настройка PowerShell для корректной работы с Git и Unicode
$ErrorActionPreference = "Stop"
$OutputEncoding = [System.Text.UTF8Encoding]::new()
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()

# Цвета для вывода
function Write-Success($msg) { Write-Host "✓ $msg" -ForegroundColor Green }
function Write-Info($msg) { Write-Host "ℹ $msg" -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Host "⚠ $msg" -ForegroundColor Yellow }
function Write-Fail($msg) { Write-Host "✗ $msg" -ForegroundColor Red; exit 1 }

Write-Info "Codex Patch Applier v2.0 для VS Code Windows"
Write-Info "=============================================="

# 1. Проверки окружения
try {
    $gitPath = (Get-Command git -ErrorAction Stop).Source
    Write-Success "Git найден: $gitPath"
} catch {
    Write-Fail "Git не установлен или не найден в PATH"
}

# Проверка что мы в Git репозитории
$gitStatus = & git rev-parse --is-inside-work-tree 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Fail "Текущая директория не является Git репозиторием"
}

# Получение информации о репозитории
$currentBranch = & git rev-parse --abbrev-ref HEAD
$repoStatus = & git status --porcelain 2>$null
$isDirty = $repoStatus.Count -gt 0

Write-Info "Ветка: $currentBranch"
Write-Info "Статус: $(if($isDirty) {'Есть изменения'} else {'Чистый'})"

if ($isDirty -and $Verbose) {
    Write-Warn "Незакоммиченные файлы:"
    $repoStatus | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
}

# 2. Получение патча из буфера обмена
Write-Info "Чтение буфера обмена..."
try {
    $clipboardRaw = Get-Clipboard -Raw
    if ([string]::IsNullOrEmpty($clipboardRaw)) {
        Write-Fail "Буфер обмена пуст"
    }
    Write-Success "Получено $($clipboardRaw.Length) символов из буфера"
} catch {
    Write-Fail "Ошибка чтения буфера обмена: $($_.Exception.Message)"
}

# 3. Предобработка патча
Write-Info "Обработка содержимого патча..."

# Нормализация переносов строк
$patchContent = $clipboardRaw -replace "`r`n", "`n" -replace "`r", "`n"

# Удаление невидимых символов Unicode
$patchContent = $patchContent -replace "[\u200B\u200C\u200D\uFEFF\u00A0]", ""

# Обработка Codex heredoc формата (cat << 'EOF')
if ($patchContent -match "(?s)cat\s*<<\s*['"]?EOF['"]?\s*\n(.*)\nEOF") {
    $patchContent = $matches[1]
    Write-Success "Извлечён патч из heredoc формата"
}

# Удаление markdown code blocks
if ($patchContent -match "(?s)^\s*```(?:diff|patch|git)?\s*\n(.*)\n```\s*$") {
    $patchContent = $matches[1]
    Write-Success "Удалены markdown code fences"
}

# Поиск начала Git патча
if ($patchContent -match "(?m)(^diff --git.*)") {
    $patchContent = $matches[1]
    Write-Success "Найден Git diff патч"
} else {
    # Сохраняем содержимое для диагностики
    $debugFile = "clipboard_debug_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"
    Set-Content -Path $debugFile -Value $clipboardRaw -Encoding UTF8
    Write-Fail "В буфере не найден валидный Git патч. Содержимое сохранено в $debugFile"
}

$patchContent = $patchContent.Trim()

# 4. Валидация патча
Write-Info "Валидация структуры патча..."
$diffCount = ([regex]::Matches($patchContent, "(?m)^diff --git")).Count
$hunkCount = ([regex]::Matches($patchContent, "(?m)^@@")).Count

Write-Info "Найдено: $diffCount файлов, $hunkCount hunks"

if ($diffCount -eq 0) {
    Write-Fail "Патч не содержит изменений файлов (diff --git)"
}

# 5. Предпросмотр патча
Write-Info "Предпросмотр патча:"
Write-Host "$(([char]0x2500) * 50)" -ForegroundColor Gray

$previewLines = $patchContent -split "`n" | Select-Object -First 15
$previewLines | ForEach-Object {
    $line = $_
    $color = "White"
    if ($line -match "^diff --git") { $color = "Magenta" }
    elseif ($line -match "^@@") { $color = "Blue" }
    elseif ($line -match "^\+") { $color = "Green" }
    elseif ($line -match "^-") { $color = "Red" }
    elseif ($line -match "^index") { $color = "Gray" }

    Write-Host $line -ForegroundColor $color
}

$totalLines = ($patchContent -split "`n").Count
if ($totalLines -gt 15) {
    Write-Host "... ещё $($totalLines - 15) строк" -ForegroundColor Gray
}
Write-Host "$(([char]0x2500) * 50)" -ForegroundColor Gray

# 6. Создание временного файла патча
$tempPatchFile = [System.IO.Path]::GetTempFileName() + ".patch"
[System.IO.File]::WriteAllText($tempPatchFile, $patchContent, [System.Text.UTF8Encoding]::new($false))
Write-Success "Патч сохранён: $tempPatchFile"

try {
    # 7. Проверка применимости
    Write-Info "Проверка применимости патча..."

    $checkOutput = & git apply --check --verbose $tempPatchFile 2>&1
    $checkSuccess = $LASTEXITCODE -eq 0

    if (-not $checkSuccess) {
        Write-Warn "Прямое применение невозможно, попробуем 3-way merge..."
        $check3way = & git apply --check --3way $tempPatchFile 2>&1  
        $check3waySuccess = $LASTEXITCODE -eq 0

        if (-not $check3waySuccess) {
            Write-Warn "Детали проблемы:"
            $checkOutput | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }

            # В VS Code можно попробовать --reject для создания .rej файлов
            $response = Read-Host "Попробовать принудительное применение с созданием .rej файлов? [y/N]"
            if ($response -notmatch "^[yYдД]") {
                Write-Fail "Применение отменено пользователем"
            }
            $forceMode = $true
        } else {
            Write-Success "3-way merge возможен"
            $use3way = $true
        }
    } else {
        Write-Success "Патч может быть применён напрямую"
    }

    # 8. Режим dry-run
    if ($DryRun) {
        Write-Success "Режим dry-run: патч выглядит применимым"
        Remove-Item $tempPatchFile -Force
        exit 0
    }

    # 9. Финальное подтверждение
    if (-not $AutoCommit) {
        Write-Host ""
        Write-Host "Готов применить патч к репозиторию:" -ForegroundColor Yellow
        Write-Host "  Ветка: $currentBranch" -ForegroundColor Yellow
        Write-Host "  Файлов: $diffCount" -ForegroundColor Yellow
        Write-Host "  Hunks: $hunkCount" -ForegroundColor Yellow
        Write-Host ""

        $confirm = Read-Host "Применить патч? [y/N]"
        if ($confirm -notmatch "^[yYдД]") {
            Write-Info "Применение отменено"
            Remove-Item $tempPatchFile -Force
            exit 0
        }
    }

    # 10. Применение патча
    Write-Info "Применение патча..."

    if ($checkSuccess) {
        & git apply $tempPatchFile
    } elseif ($use3way) {
        & git apply --3way $tempPatchFile
    } elseif ($forceMode) {
        & git apply --reject --whitespace=fix $tempPatchFile
    }

    $applySuccess = $LASTEXITCODE -eq 0

    if ($applySuccess) {
        Write-Success "Патч применён успешно!"
    } else {
        Write-Warn "Патч применён с предупреждениями"
    }

    # 11. Показать результат
    Write-Info "Изменения в рабочей директории:"
    & git status --short | ForEach-Object {
        $status = $_.Substring(0,2)
        $file = $_.Substring(3)
        $color = switch ($status.Trim()) {
            "M" { "Yellow" }
            "A" { "Green" }  
            "D" { "Red" }
            "??" { "Gray" }
            default { "White" }
        }
        Write-Host "  $status $file" -ForegroundColor $color
    }

    # 12. Автокоммит (если включён)
    if ($AutoCommit) {
        Write-Info "Автоматический коммит изменений..."
        & git add -A

        $commitMsg = "Apply Codex patch: $diffCount files, $hunkCount hunks"
        & git commit -m $commitMsg

        if ($LASTEXITCODE -eq 0) {
            Write-Success "Изменения закоммичены: $commitMsg"
        } else {
            Write-Warn "Коммит не выполнен (возможно нет изменений)"
        }
    } else {
        Write-Info "Используйте 'git add' и 'git commit' для сохранения изменений"
    }

} finally {
    # Очистка временных файлов
    if (Test-Path $tempPatchFile) {
        Remove-Item $tempPatchFile -Force
    }
}

Write-Success "Операция завершена!"