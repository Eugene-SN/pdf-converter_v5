## 1. CLI Entrypoints
- `convert-pdf-to-markdown.sh` теперь пишет в единый лог и отражает выбранное значение OCR, но оркестратору всё ещё отправляется `enable_ocr` из конфигурации без учёта фактического анализа Stage 1. 【F:convert-pdf-to-markdown.sh†L47-L106】
- `translate-documents.sh` также пишет в один лог, но остаётся интерактивным (меню) и всегда включает OCR (`"enable_ocr": true`) при запуске полного конвейера, что нарушает требование последовательного подтверждения предыдущих этапов. 【F:translate-documents.sh†L79-L214】

## 2. Document Preprocessing DAG
- `validate_input_file` помечает цифровой PDF как `has_chinese_text: False` и `recommended_ocr: True`, автоматически возвращая конфигурацию с включённым OCR. 【F:logs/dag_id=document_preprocessing/run_id=manual__2025-09-27T21:39:58.81851300:00/task_id=validate_input_file/attempt=1.log†L32-L38】
- `process_document_with_api` передаёт в document-processor опции с `use_ocr: True`, что подтверждает фактическое принудительное включение OCR и приводит к излишней нагрузке. 【F:logs/dag_id=document_preprocessing/run_id=manual__2025-09-27T21:39:58.81851300:00/task_id=process_document_with_api/attempt=1.log†L11-L12】

## 3. Content Transformation DAG
- Базовые китайские трансформации всегда вызывают `enhance_chinese_tables` и `clean_chinese_formatting`, которые модифицируют блоки таблиц без доступа к структурным данным; `_normalize_table_block` вставляет разделители даже в уже корректные таблицы. 【F:airflow/dags/content_transformation.py†L624-L724】
- vLLM-этап обрабатывает 5 чанков последовательно; каждый API вызов занимает до 20–40 секунд, а качество маркируется как 1.000 без сверки с исходным контентом. 【F:logs/dag_id=content_transformation/run_id=manual__2025-09-27T21:40:32.15917600:00/task_id=perform_vllm_enhancement/attempt=1.log†L10-L39】

## 4. Quality Assurance DAG
- Визуальная проверка (Level 2) пропущена из-за недоступности Docker Pandoc; отчёт не содержит SSIM и расхождений. 【F:logs/dag_id=quality_assurance/run_id=manual__2025-09-27T21:43:07.57498000:00/task_id=perform_visual_comparison/attempt=1.log†L10-L15】
- Уровень 5 запускает vLLM-исправления и вносит две правки с уверенностью 1.0, что может менять содержимое без подтверждения. 【F:logs/dag_id=quality_assurance/run_id=manual__2025-09-27T21:43:07.57498000:00/task_id=perform_auto_correction/attempt=1.log†L10-L12】
- Финализация QA показывает лишь 89.3 % качества, хотя пайплайн помечает прогон как пройденный. 【F:logs/dag_id=quality_assurance/run_id=manual__2025-09-27T21:43:07.57498000:00/task_id=finalize_qa_process/attempt=1.log†L10-L12】

## 5. Markdown Output vs Source PDF
- Основная таблица команд разорвана: строки дублируются, разделители расходятся, что делает Markdown нечитаемым. 【F:output/zh/1759009195_test_short.md†L5-L33】
- Блоки с описаниями команд содержат «|------|» и обрезанные столбцы, а кодовые примеры повреждены. 【F:output/zh/1759009195_test_short.md†L77-L118】

## 6. Translator Service (`translator/translator.py`)
- vLLM используется как основной движок, но отсутствует защита таблиц и кодовых блоков в промптах, а параметры батчинга и кэша не проверяются тестами. 【F:translator/translator.py†L1-L160】
- Таймауты (`REQUEST_TIMEOUT=600`) и агрессивные batch размеры при отсутствии троттлинга могут приводить к длинным зависаниям, что видно по длительности автокоррекции. 【F:translator/translator.py†L49-L86】【F:logs/dag_id=quality_assurance/run_id=manual__2025-09-27T21:43:07.57498000:00/task_id=perform_auto_correction/attempt=1.log†L10-L12】

## 7. Observability & Monitoring
- CLI лог (`conversion_20250928_004615.log`) фиксирует итоговые размеры и базовую статистику, но не связывает события Stage 1/2/3 в одном файле, усложняя трассировку. 【F:logs/conversion_20250928_004615.log†L1-L10】
- Prometheus/Grafana не покрывают ключевые метрики точности (визуальная проверка отключена), что расходится с целями исходной архитектуры. 【F:docs/original_architecture_summary.md†L6-L18】【F:logs/dag_id=quality_assurance/run_id=manual__2025-09-27T21:43:07.57498000:00/task_id=perform_visual_comparison/attempt=1.log†L10-L15】

## 8. Baseline Requirement
- Итоговый Markdown пока далеко от требуемых 100 % соответствия, поэтому дальнейшие изменения должны выполняться строго последовательно, с фиксацией результатов каждого этапа и регрессионными проверками перед переходом к следующему модулю. 【F:output/zh/1759009195_test_short.md†L5-L118】【F:logs/dag_id=quality_assurance/run_id=manual__2025-09-27T21:43:07.57498000:00/task_id=finalize_qa_process/attempt=1.log†L10-L12】
