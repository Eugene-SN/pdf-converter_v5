#!/bin/bash

# ===============================================================================
# PDF CONVERTER PIPELINE v2.0 - ИСПРАВЛЕНА ПРОБЛЕМА JSON ФОРМАТИРОВАНИЯ
# Объединенный скрипт для полной конвертации PDF в Markdown с 5-уровневой валидацией
# Результат: PDF → Markdown с качеством 100% в папке output_md_zh
# ===============================================================================

set -euo pipefail

# Конфигурация
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/.env"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Загрузка конфигурации
if [ -f "$CONFIG_FILE" ]; then
    source "$CONFIG_FILE"
fi

# Конфигурация сервисов
AIRFLOW_URL="${AIRFLOW_BASE_URL_HOST:-http://localhost:8090}"
AIRFLOW_USERNAME="${AIRFLOW_USERNAME:-admin}"
AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-admin}"

# Локальные папки
HOST_INPUT_DIR="${SCRIPT_DIR}/input"
HOST_OUTPUT_DIR="${SCRIPT_DIR}/output/zh"
HOST_METADATA_DIR="${SCRIPT_DIR}/output/metadata"
LOGS_DIR="${SCRIPT_DIR}/logs"

# Настройки конвертации
CONVERSION_BACKEND="${CONVERSION_BACKEND:-local}"
CONVERTER_ENABLE_OCR_BOOL="${CONVERTER_ENABLE_OCR_BOOL:-false}"
LOCAL_ALLOW_OCR_FALLBACK="${LOCAL_ALLOW_OCR_FALLBACK:-false}"
LOCAL_EXPORT_RAW_TEXT="${LOCAL_EXPORT_RAW_TEXT:-false}"
LOCAL_OVERWRITE_OUTPUT="${LOCAL_OVERWRITE_OUTPUT:-false}"
LOCAL_CONVERTER_VERBOSE="${LOCAL_CONVERTER_VERBOSE:-false}"
LOCAL_OCR_LANGUAGES="${LOCAL_OCR_LANGUAGES:-eng}"
LOCAL_CONVERTER_MODULE="document_processor.local_converter"

# Создание директорий
mkdir -p "$HOST_INPUT_DIR" "$HOST_OUTPUT_DIR" "$HOST_METADATA_DIR" "$LOGS_DIR"

# Настройки логирования для одного запуска
LOG_FILE="${LOGS_DIR}/conversion_$(date +%Y%m%d_%H%M%S)_$$.log"
touch "$LOG_FILE"

log_file_path() {
    echo "$LOG_FILE"
}

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${BLUE}[$timestamp]${NC} ${YELLOW}[$level]${NC} $message" | tee -a "$(log_file_path)" >&2
}

show_header() {
    echo -e "${BLUE}"
    echo "==============================================================================="
    echo " PDF CONVERTER PIPELINE v2.0 - ИСПРАВЛЕНА JSON ПРОБЛЕМА"
    echo "==============================================================================="
    echo -e "${NC}"
    echo "🎯 Цель: PDF → Markdown с 5-уровневой валидацией качества"
    echo "📂 Входная папка: $HOST_INPUT_DIR"
    echo "📁 Выходная папка: $HOST_OUTPUT_DIR"
    echo "🔄 Этапы:"
    echo " 1️⃣ Document Preprocessing (Извлечение контента)"
    echo " 2️⃣ Content Transformation (Преобразование в Markdown)"
    echo " 3️⃣ Quality Assurance (5-уровневая валидация)"
    echo ""
    echo "⚙️ Режим работы: $CONVERSION_BACKEND"
    if [[ "$CONVERSION_BACKEND" == "local" ]]; then
        echo "   ➤ Используется локальный Docling конвертер без OCR по умолчанию"
    else
        echo "   ➤ Используется Airflow оркестратор"
    fi
    echo ""
}

check_services() {
    log "INFO" "Проверка готовности сервисов..."
    local services=(
        "$AIRFLOW_URL/health:Airflow UI"
    )

    for service_info in "${services[@]}"; do
        local url="${service_info%:*}"
        local name="${service_info#*:}"
        if curl -s --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" "$url" > /dev/null 2>&1; then
            log "INFO" "✅ $name готов"
        else
            # Fallback на Airflow API v2 health endpoint
            local base="${url%/health}"
            local v2="${base}/api/v2/monitor/health"
            if ! curl -s --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" "$v2" > /dev/null 2>&1; then
                log "ERROR" "$name недоступен на $url"
                exit 1
            fi
            log "INFO" "✅ $name готов (через API v2)"
        fi
    done

    log "INFO" "✅ Все сервисы готовы"
}

run_local_conversion() {
    local pdf_file="$1"
    local filename
    filename=$(basename "$pdf_file")

    log "INFO" "🚀 Локальная конвертация: $filename"

    local converter_cmd=(
        python3 -m "$LOCAL_CONVERTER_MODULE"
        --input "$pdf_file"
        --output-dir "$HOST_OUTPUT_DIR"
        --metadata-dir "$HOST_METADATA_DIR"
        --ocr-languages "$LOCAL_OCR_LANGUAGES"
    )

    if [[ "$CONVERTER_ENABLE_OCR_BOOL" == "true" ]]; then
        converter_cmd+=(--enable-ocr)
        if [[ "$LOCAL_ALLOW_OCR_FALLBACK" == "true" ]]; then
            converter_cmd+=(--allow-ocr-fallback)
        fi
    else
        if [[ "$LOCAL_ALLOW_OCR_FALLBACK" == "true" ]]; then
            converter_cmd+=(--allow-ocr-fallback)
        fi
    fi

    if [[ "$LOCAL_EXPORT_RAW_TEXT" == "true" ]]; then
        converter_cmd+=(--export-raw-text)
    fi

    if [[ "$LOCAL_OVERWRITE_OUTPUT" == "true" ]]; then
        converter_cmd+=(--overwrite)
    fi

    if [[ "$LOCAL_CONVERTER_VERBOSE" == "true" ]]; then
        converter_cmd+=(--verbose)
    fi

    local converter_output
    if ! converter_output=$("${converter_cmd[@]}"); then
        local exit_code=$?
        log "ERROR" "❌ Локальный конвертер завершился с ошибкой (код $exit_code)"
        log "ERROR" "Вывод: $converter_output"
        return $exit_code
    fi

    local status
    status=$(echo "$converter_output" | jq -r '.status // empty' 2>/dev/null || echo "")
    if [[ "$status" != "success" ]]; then
        log "ERROR" "❌ Локальный конвертер вернул ошибку: $converter_output"
        return 1
    fi

    local markdown_path
    markdown_path=$(echo "$converter_output" | jq -r '.output_markdown // empty')
    local metadata_path
    metadata_path=$(echo "$converter_output" | jq -r '.metadata_file // empty')
    local raw_text_path
    raw_text_path=$(echo "$converter_output" | jq -r '.raw_text_file // empty')
    local pages sections
    pages=$(echo "$converter_output" | jq -r '.pages // "0"')
    sections=$(echo "$converter_output" | jq -r '.sections // "0"')

    log "INFO" "📄 Markdown: ${markdown_path:-не указан}"
    if [[ -n "$metadata_path" ]]; then
        log "INFO" "🧾 Метаданные: $metadata_path"
    fi
    if [[ -n "$raw_text_path" ]]; then
        log "INFO" "📝 Текст: $raw_text_path"
    fi
    log "INFO" "📊 Страниц: $pages, секций: $sections"

    printf '%s\n' "$converter_output"
}

# ИСПРАВЛЕНО: Правильное формирование JSON через jq
trigger_full_conversion() {
    local pdf_file="$1"
    local filename
    filename=$(basename "$pdf_file")
    local timestamp
    timestamp=$(date +%s)

    log "INFO" "🚀 Запуск полной конвертации: $filename"
    local enable_ocr_flag="$CONVERTER_ENABLE_OCR_BOOL"
    if [[ "$enable_ocr_flag" == "true" ]]; then
        log "INFO" "⚙️ OCR включен во входной конфигурации"
    else
        log "INFO" "⚙️ OCR отключен во входной конфигурации"
    fi

    # ИСПРАВЛЕНО: Используем jq для корректного JSON
    local config_json
    config_json=$(jq -n \
        --arg input_file "$pdf_file" \
        --arg filename "$filename" \
        --argjson timestamp $timestamp \
        --arg target_language "original" \
        --arg quality_level "high" \
        --argjson enable_ocr "$enable_ocr_flag" \
        --argjson preserve_structure true \
        --argjson extract_tables true \
        --argjson extract_images true \
        --arg stage_mode "full_conversion_with_validation" \
        --argjson processing_stages 4 \
        --argjson validation_enabled true \
        --argjson quality_target 100.0 \
        --arg language "zh-CN" \
        --argjson chinese_optimization true \
        --arg pipeline_version "4.0" \
        --arg processing_mode "digital_pdf" \
        --argjson use_orchestrator true \
        '{
            input_file: $input_file,
            filename: $filename,
            timestamp: $timestamp,
            target_language: $target_language,
            quality_level: $quality_level,
            enable_ocr: $enable_ocr,
            preserve_structure: $preserve_structure,
            extract_tables: $extract_tables,
            extract_images: $extract_images,
            stage_mode: $stage_mode,
            processing_stages: $processing_stages,
            validation_enabled: $validation_enabled,
            quality_target: $quality_target,
            language: $language,
            chinese_optimization: $chinese_optimization,
            pipeline_version: $pipeline_version,
            processing_mode: $processing_mode,
            use_orchestrator: $use_orchestrator
        }')

    # ИСПРАВЛЕНО: Правильная структура запроса для Airflow API
    local request_body
    request_body=$(jq -n --argjson conf "$config_json" '{conf: $conf}')

    log "INFO" "📤 Отправка запроса в Airflow..."
    #log "DEBUG" "JSON config: $config_json"

    # ИСПРАВЛЕНО: Запуск через orchestrator с правильным JSON
    local response
    response=$(curl -s -w "\n%{http_code}" \
        -X POST \
        --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
        -H "Content-Type: application/json" \
        -d "$request_body" \
        "$AIRFLOW_URL/api/v1/dags/orchestrator_dag/dagRuns")

    local http_code
    http_code=$(echo "$response" | tail -n1)
    local body
    body=$(echo "$response" | head -n -1)

    if [ "$http_code" -eq 200 ] || [ "$http_code" -eq 201 ]; then
        local dag_run_id
        dag_run_id=$(echo "$body" | jq -r '.dag_run_id // "unknown"' 2>/dev/null || echo "unknown")
        log "INFO" "✅ Конвертация запущена. Run ID: $dag_run_id"

        # Мониторинг выполнения
        wait_for_completion "$dag_run_id" "$filename"
        return 0
    else
        log "ERROR" "❌ Ошибка запуска конвертации: HTTP $http_code"
        log "ERROR" "Ответ: $body"

        # Дополнительная диагностика
        if [[ "$body" == *"not valid JSON"* ]]; then
            log "ERROR" "🔧 Проблема с JSON форматированием. Проверьте установку jq:"
            log "ERROR" "   sudo apt-get install jq"
        elif [[ "$body" == *"orchestrator_dag"* ]]; then
            log "ERROR" "🔧 orchestrator_dag недоступен. Проверьте DAG в Airflow UI:"
            log "ERROR" "   $AIRFLOW_URL/dags/orchestrator_dag"
        fi

        return 1
    fi
}

wait_for_completion() {
    local dag_run_id="$1"
    local filename="$2"
    local timeout=3600  # 1 час
    local start_time
    start_time=$(date +%s)

    log "INFO" "⏳ Ожидание завершения полной конвертации (таймаут: ${timeout}s)..."

    while true; do
        local current_time
        current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ $elapsed -gt $timeout ]; then
            log "ERROR" "❌ Таймаут конвертации"
            return 1
        fi

        # Получение статуса orchestrator DAG
        local response
        response=$(curl -s \
            --user "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD" \
            "$AIRFLOW_URL/api/v1/dags/orchestrator_dag/dagRuns/$dag_run_id")

        local state
        state=$(echo "$response" | jq -r '.state // "unknown"' 2>/dev/null || echo "error")

        case "$state" in
            "success")
                log "INFO" "✅ Конвертация завершена успешно!"
                show_completion_results "$filename"
                return 0
                ;;
            "failed"|"upstream_failed")
                log "ERROR" "❌ Конвертация завершена с ошибкой"
                log "ERROR" "🔍 Проверьте детали в Airflow UI: $AIRFLOW_URL/dags/orchestrator_dag/grid?dag_run_id=$dag_run_id"
                return 1
                ;;
            "running")
                local progress_msg="Выполняется (${elapsed}s)"
                printf "\r${YELLOW}[КОНВЕРТАЦИЯ]${NC} $progress_msg " >&2
                sleep 10
                ;;
            *)
                sleep 5
                ;;
        esac
    done
}

show_completion_results() {
    local filename="$1"

    log "INFO" "📊 Результаты конвертации:"

    # Поиск результирующего файла
    local latest_file
    latest_file=$(find "$HOST_OUTPUT_DIR" -name "*.md" -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2- 2>/dev/null || echo "")

    if [ -n "$latest_file" ] && [ -f "$latest_file" ]; then
        log "INFO" "📁 Результирующий файл: $latest_file"
        local file_size
        file_size=$(wc -c < "$latest_file" 2>/dev/null || echo "0")
        log "INFO" "📊 Размер файла: $file_size байт"

        # Базовая статистика
        local lines words
        lines=$(wc -l < "$latest_file" 2>/dev/null || echo "0")
        words=$(wc -w < "$latest_file" 2>/dev/null || echo "0")

        log "INFO" "📈 Статистика:"
        log "INFO" "  - Строк: $lines"
        log "INFO" "  - Слов: $words"
        log "INFO" "  - Символов: $file_size"

        # Проверка качества (базовая)
        if [ "$file_size" -gt 100 ]; then
            log "INFO" "✅ Качество: Файл содержит достаточно контента"

            # Показать первые несколько строк
            log "INFO" "📖 Превью содержимого:"
            while IFS= read -r preview_line; do
                log "INFO" "  $preview_line"
            done < <(head -5 "$latest_file")
        else
            log "WARN" "⚠️ Качество: Файл может быть слишком коротким"
        fi
    else
        log "WARN" "⚠️ Результирующий файл не найден в $HOST_OUTPUT_DIR"

        # Попытка найти любые недавние файлы
        log "INFO" "🔍 Поиск любых файлов в выходной папке..."
        find "$HOST_OUTPUT_DIR" -type f -name "*.md" -mmin -60 2>/dev/null | head -5 | while read -r file; do
            if [ -n "$file" ]; then
                log "INFO" "  Найден: $file"
            fi
        done
    fi
}

process_batch() {
    log "INFO" "🔍 Поиск PDF файлов для конвертации..."

    # Поиск всех PDF файлов
    local pdf_files=()
    while IFS= read -r -d '' file; do
        pdf_files+=("$file")
    done < <(find "$HOST_INPUT_DIR" -name "*.pdf" -type f -print0)

    local total_files=${#pdf_files[@]}

    if [ $total_files -eq 0 ]; then
        log "WARN" "📂 Нет PDF файлов в $HOST_INPUT_DIR"
        log "INFO" "Поместите PDF файлы в папку $HOST_INPUT_DIR и запустите снова"
        return 0
    fi

    log "INFO" "📊 Найдено файлов для конвертации: $total_files"
    >&2 echo ""

    # Обработка файлов
    local processed=0
    local failed=0
    local start_time
    start_time=$(date +%s)
    local success_summaries=()

    for pdf_file in "${pdf_files[@]}"; do
        local filename
        filename=$(basename "$pdf_file")
        echo -e "${BLUE}[ФАЙЛ $((processed + failed + 1))/$total_files]${NC} $filename"

        if [[ "$CONVERSION_BACKEND" == "local" ]]; then
            local conversion_json
            if conversion_json=$(run_local_conversion "$pdf_file"); then
                ((processed++))
                success_summaries+=("$conversion_json")
                echo -e "Статус: ${GREEN}✅ УСПЕШНО КОНВЕРТИРОВАН${NC}"
            else
                ((failed++))
                echo -e "Статус: ${RED}❌ ОШИБКА КОНВЕРТАЦИИ${NC}"
            fi
        else
            if trigger_full_conversion "$pdf_file"; then
                ((processed++))
                echo -e "Статус: ${GREEN}✅ УСПЕШНО КОНВЕРТИРОВАН${NC}"
            else
                ((failed++))
                echo -e "Статус: ${RED}❌ ОШИБКА КОНВЕРТАЦИИ${NC}"
            fi
        fi
        >&2 echo ""
    done

    # Итоговая статистика
    local end_time
    end_time=$(date +%s)
    local total_duration=$((end_time - start_time))

    {
        echo "==============================================================================="
        echo -e "${GREEN}ПОЛНАЯ КОНВЕРТАЦИЯ ЗАВЕРШЕНА${NC}"
        echo "==============================================================================="
        echo -e "📊 Статистика обработки:"
        echo -e " Успешно конвертировано: ${GREEN}$processed${NC} файлов"
        echo -e " Ошибок: ${RED}$failed${NC} файлов"
        echo -e " Общее время: ${BLUE}$total_duration${NC} секунд"
        echo ""
        echo -e "📁 Результаты сохранены в: ${YELLOW}$HOST_OUTPUT_DIR${NC}"
        echo -e "📋 Логи сохранены в: ${YELLOW}$LOGS_DIR${NC}"
        echo ""
    } >&2

    if [ $failed -gt 0 ]; then
        echo -e "${YELLOW}⚠️ Диагностика проблем:${NC}"
        if [[ "$CONVERSION_BACKEND" == "local" ]]; then
            echo " - Проверьте логи конвертера: $(log_file_path)"
            echo " - Убедитесь, что зависимости docling установлены"
            echo " - Попробуйте включить LOCAL_CONVERTER_VERBOSE=true для подробностей"
        else
            echo " - Проверьте Airflow UI: $AIRFLOW_URL/dags"
            echo " - Убедитесь что orchestrator_dag активен"
            echo " - Проверьте логи: $LOGS_DIR/conversion_*.log"
            echo " - Проверьте статус всех DAG в проекте"
        fi
    else
        echo -e "${GREEN}🎉 Все файлы успешно конвертированы!${NC}" >&2
        >&2 echo ""
        echo "Следующие шаги:" >&2
        echo " - Файлы готовы к использованию" >&2
        echo " - Для перевода: ./translate-documents.sh [язык]" >&2
    fi

    if [[ "$CONVERSION_BACKEND" == "local" && ${#success_summaries[@]} -gt 0 ]]; then
        >&2 echo ""
        echo "📌 Конвертированные файлы:" >&2
        for summary in "${success_summaries[@]}"; do
            local md_path metadata_path raw_text_path
            md_path=$(echo "$summary" | jq -r '.output_markdown')
            metadata_path=$(echo "$summary" | jq -r '.metadata_file // empty')
            raw_text_path=$(echo "$summary" | jq -r '.raw_text_file // empty')
            echo " - Markdown: $md_path" >&2
            if [[ -n "$metadata_path" ]]; then
                echo "   Метаданные: $metadata_path" >&2
            fi
            if [[ -n "$raw_text_path" ]]; then
                echo "   Текст: $raw_text_path" >&2
            fi
        done
        >&2 echo ""
        echo "Для повторной конвертации можно установить LOCAL_OVERWRITE_OUTPUT=true" >&2
    fi

    if [[ "$CONVERSION_BACKEND" == "local" && ${#success_summaries[@]} -gt 0 ]]; then
        echo ""
        echo "📌 Конвертированные файлы:"
        for summary in "${success_summaries[@]}"; do
            local md_path metadata_path raw_text_path
            md_path=$(echo "$summary" | jq -r '.output_markdown')
            metadata_path=$(echo "$summary" | jq -r '.metadata_file // empty')
            raw_text_path=$(echo "$summary" | jq -r '.raw_text_file // empty')
            echo " - Markdown: $md_path"
            if [[ -n "$metadata_path" ]]; then
                echo "   Метаданные: $metadata_path"
            fi
            if [[ -n "$raw_text_path" ]]; then
                echo "   Текст: $raw_text_path"
            fi
        done
        echo ""
        echo "Для повторной конвертации можно установить LOCAL_OVERWRITE_OUTPUT=true"
    fi
}

# Проверка зависимостей
check_dependencies() {
    log "INFO" "🔧 Проверка зависимостей..."

    # Проверка jq
    if ! command -v jq &> /dev/null; then
        log "ERROR" "❌ jq не установлен. Установите: sudo apt-get install jq"
        exit 1
    fi
    log "INFO" "✅ jq установлен"

    if [[ "$CONVERSION_BACKEND" == "local" ]]; then
        if ! command -v python3 &> /dev/null; then
            log "ERROR" "❌ Python 3 не найден. Установите python3"
            exit 1
        fi
        if ! python3 - >/dev/null 2>&1 <<'PY'; then
import importlib.util
required = ["docling", "docling_core"]
missing = [name for name in required if importlib.util.find_spec(name) is None]
if missing:
    raise SystemExit(1)
PY
            log "ERROR" "❌ Отсутствуют зависимости docling/docling_core. Установите их перед запуском"
            exit 1
        fi
        log "INFO" "✅ Python окружение готово"
    else
        if ! command -v curl &> /dev/null; then
            log "ERROR" "❌ curl не установлен. Установите: sudo apt-get install curl"
            exit 1
        fi
        log "INFO" "✅ curl установлен"
    fi
}

# Основная логика
main() {
    show_header
    log "INFO" "Запись лога: $(log_file_path)"
    check_dependencies
    if [[ "$CONVERSION_BACKEND" == "local" ]]; then
        log "INFO" "Проверка сервисов не требуется для локального режима"
    else
        check_services
    fi

    if [[ "$CONVERSION_BACKEND" == "local" ]]; then
        echo -e "${YELLOW}Начинаем локальную конвертацию PDF → Markdown (OCR отключен по умолчанию)${NC}"
    else
        echo -e "${YELLOW}Начинаем полную конвертацию PDF → Markdown с валидацией${NC}"
    fi
    echo -e "${YELLOW}Нажмите Enter для начала или Ctrl+C для отмены...${NC}"
    read -r

    process_batch
}

# Запуск, если скрипт вызван напрямую
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
