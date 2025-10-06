# PDF Converter Pipeline v2.0 - Модульная Архитектура

> Высокоточная конвертация **цифровых PDF** в Markdown с модульной оркестрацией, 5-уровневым контролем качества и опциональным переводом с китайского на английский или русский языки.

## 📚 Содержание
- [🚀 Обзор системы](#-обзор-системы)
- [⭐ Ключевые особенности](#-ключевые-особенности)
- [🏛️ Архитектура системы](#️-архитектура-системы)
- [🔄 Модульные DAG](#-модульные-dag)
- [🧩 Модули репозитория](#-модули-репозитория)
  - [document_processor](#document_processor)
  - [translator](#translator)
  - [quality_assurance](#quality_assurance)
  - [Инфраструктура и сервисы](#инфраструктура-и-сервисы)
- [⚙️ Этапы обработки документа](#️-этапы-обработки-документа)
- [🛠️ Требования системы](#️-требования-системы)
  - [Минимальная конфигурация](#минимальная-конфигурация)
  - [Рекомендуемая конфигурация](#рекомендуемая-конфигурация)
- [📦 Быстрая установка](#-быстрая-установка)
  - [Автоматическая установка](#автоматическая-установка)
  - [Ручная установка](#ручная-установка)
- [🌐 Веб-интерфейсы](#-веб-интерфейсы)
- [🔧 Использование системы](#-использование-системы)
  - [API и пакетная обработка](#api-и-пакетная-обработка)
  - [Веб-интерфейс](#веб-интерфейс)
  - [Мониторинг обработки](#мониторинг-обработки)
- [📊 Мониторинг и метрики](#-мониторинг-и-метрики)
- [🔍 Устранение неполадок](#-устранение-неполадок)
- [🗂️ Структура выходных файлов](#️-структура-выходных-файлов)
- [🔄 Управление и масштабирование](#-управление-и-масштабирование)
- [🛡️ Безопасность](#️-безопасность)
- [🚀 Производительность](#-производительность)
- [📚 Дополнительная документация](#-дополнительная-документация)
- [🤝 Вклад в проект](#-вклад-в-проект)
- [📄 Лицензия](#-лицензия)
- [🆘 Поддержка](#-поддержка)
- [🎯 Roadmap](#-roadmap)

## 🚀 Обзор системы

**PDF Converter Pipeline v2.0** — высокопроизводительная система, разработанная для цифровых PDF-документов и адаптированная под многоязычную локализацию. Пайплайн сочетает гибкую модульную архитектуру (4 DAG + Master Orchestrator), интеллектуальное извлечение структуры (IBM Docling), комбинированный OCR (PaddleOCR + Tesseract) для гибридных сценариев и 5-уровневую систему QA для достижения качества вплоть до 100 %.

Проект масштабируется от локального запуска через `convert-pdf-to-markdown.sh` до распределённого исполнения в кластере Airflow, а перевод выполняется средствами vLLM с поддержкой китайского, английского и русского языков.

## ⭐ Ключевые особенности

- 🧠 **vLLM Integration** — использование высокопроизводительных inference-серверов vLLM (замена Ollama) для переводов и QA-промптов.
- 🏗️ **Модульная архитектура** — Master Orchestrator управляет четырьмя специализированными DAG: предварительная обработка, преобразование, перевод, контроль качества.
- 📄 **Docling + OCR** — гибридный стек IBM Docling + PaddleOCR + Tesseract + Tabula-Py обеспечивает точное извлечение текста, таблиц и изображений.
- 🔍 **5-уровневая QA система** — комбинированная проверка (OCR cross-check, визуальный diff, AST-анализ, содержательная валидация, автокоррекция).
- 🌐 **Многоязычность** — китайский → английский/русский с сохранением команд IPMI/BMC/Redfish и технических терминов.
- 📊 **Полный мониторинг** — Prometheus + Grafana с кастомными дашбордами и alerting.
- ⚡ **GPU оптимизация** — нативная поддержка 2× NVIDIA A6000 (или A100/H100) под CUDA 12.9.

## 🏛️ Архитектура системы

```
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ Flask API       │ │ Master DAG      │ │ Monitoring      │
│ (Gateway)       │◄──►│ (Orchestrator)  │◄──►│ Prometheus+     │
└─────────────────┘ └─────────────────┘ │ Grafana         │
                 │                     └─────────────────┘
                 ▼
┌───────────────────────────────────────────────────────┐
│ Modular DAG Pipeline                                  │
└───────────────────────────────────────────────────────┘
                 │
┌───────────────────────┼───────────────────────────────┐
▼                       ▼                               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ DAG 1        │ │ DAG 2        │ │ DAG 3        │ │ DAG 4        │
│ Document     │───►│ Content      │───►│ Translation  │───►│ Quality      │
│ Preprocessing│ │Transformation│ │ Pipeline     │ │ Assurance    │
│              │ │              │ │              │ │ (5 levels)   │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
       │                │                │                │
       ▼                ▼                ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Docling +    │ │ vLLM         │ │ vLLM         │ │ OCR Cross-   │
│ PaddleOCR +  │ │ Qwen2.5-VL   │ │ Qwen3-30B-   │ │ Validation + │
│ Tesseract +  │ │ Markdown     │ │ A3B-Instruct │ │ Visual Diff +│
│ Tabula-Py    │ │ Generation   │ │ Translation  │ │ AST Compare +│
│              │ │              │ │              │ │ Auto-Correct │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
```

Master Orchestrator (через Airflow или локальный контроллер) координирует прохождение документов по всем этапам, собирает телеметрию и публикует события в систему мониторинга. Для локальной разработки каждый DAG может быть вызван напрямую через соответствующие скрипты репозитория.

## 🔄 Модульные DAG

### DAG 1: Document Preprocessing
- **Цель**: Чистое извлечение контента без потерь структуры.
- **Компоненты**: IBM Docling, PaddleOCR + Tesseract cross-validation, Tabula-Py для таблиц.
- **Выходы**: Структурированный контент, метаданные, промежуточные артефакты (изображения, таблицы, статистика).

### DAG 2: Content Transformation
- **Цель**: Генерация высококачественного Markdown.
- **Компоненты**: vLLM с Qwen2.5-VL-32B-Instruct, нормализация Markdown, постобработка.
- **Выходы**: Markdown с сохранением семантики и технических терминов.

### DAG 3: Translation Pipeline
- **Цель**: Перевод с сохранением структуры и команд.
- **Компоненты**: vLLM с Qwen3-30B-A3B-Instruct-2507, промпты для технической документации.
- **Выходы**: Переведённые Markdown-файлы (EN/RU) + сопутствующие метаданные.

### DAG 4: Quality Assurance (5 уровней)
- **Уровень 1**: OCR cross-validation (PaddleOCR + Tesseract).
- **Уровень 2**: Визуальное сравнение + SSIM-анализ.
- **Уровень 3**: AST сравнение структуры документов.
- **Уровень 4**: Валидация содержимого (таблицы, код, изображения, термины).
- **Уровень 5**: Автокоррекция, итоговая оценка (до 100 %).

## 🧩 Модули репозитория

### `document_processor`

- **`docling_processor.py`** — адаптер Docling, управление chunking-стратегиями, экспорт изображений/таблиц, включение OCR по запросу.
- **`local_converter.py`** — CLI/оркестратор локальной конвертации; генерирует Markdown, сохраняет активы, формирует JSON-ответы.
- **`__init__.py`** — экспорт публичных классов и конфигураций.
- Дополнительные вспомогательные файлы (OCR, таблицы, анализ структуры) интегрируются по мере развертывания v2.0.

### `translator`

- **`translator.py`** — CLI и библиотечный интерфейс для перевода Markdown через vLLM.
- **`translation_prompts.py`** — шаблоны промптов для Qwen3-30B-A3B-Instruct.
- **`config.py`** — параметры подключения к моделям, температуре, длине контекста.
- **`requirements-translator.txt`** — зависимости переводческого окружения.

### `quality_assurance`

- Набор проверок и утилит для 5-уровневой QA: верификация ссылок, анализ AST, сопоставление объёма, визуальное сравнение.
- Интеграция с промптовыми проверками vLLM и хранение отчетов `qa_report.json`.

### Инфраструктура и сервисы

- **`airflow/`** — DAG Master Orchestrator и специализированные DAG 1–4.
- **`flask/`** — REST API (gateway) для загрузки PDF и управления заданиями.
- **`grafana/`, `prometheus/`** — мониторинг (дашборды, алерты).
- **`vllm/`** — конфигурация inference-серверов и загрузка моделей Qwen.
- **`docs/`** — архитектурные диаграммы, deployment-guide и дополнительные схемы.

## ⚙️ Этапы обработки документа

1. **Подготовка** — PDF помещаются в `input_pdf/` или загружаются через Flask API.
2. **Запуск конвертации** — `convert-pdf-to-markdown.sh` (локально) либо Master DAG в Airflow инициируют выполнение `document_processor.local_converter`.
3. **Docling-преобразование** — извлечение структуры, таблиц, изображений (без OCR по умолчанию, OCR включается опционально).
4. **Нормализация Markdown** — очистка форматирования, сохранение активов в `output_md_zh/…/images`.
5. **Метаданные** — генерация `metadata.json` с статистикой и служебными полями (номер страниц, успешность извлечения таблиц).
6. **QA Pipeline** — запуск модулей `quality_assurance` для проверки целостности.
7. **Перевод (опционально)** — `translate-documents.sh` вызывает `translator` для генерации EN/RU версий с помощью vLLM.
8. **Отчётность** — результаты публикуются в Prometheus, отображаются в Grafana и доступны в Airflow UI.

## 🛠️ Требования системы

### Минимальная конфигурация
- **ОС**: Ubuntu 22.04 LTS+
- **RAM**: 32 GB+ (рекомендуется 64 GB)
- **Storage**: 500 GB свободного места
- **GPU**: 2× NVIDIA A6000 (24 GB VRAM каждая) или эквивалент
- **CUDA**: 12.9+
- **Docker**: 24.0+
- **Docker Compose**: 2.20+

### Рекомендуемая конфигурация
- **RAM**: 128 GB
- **Storage**: 1 TB NVMe SSD
- **GPU**: 2× NVIDIA H100 / A100
- **Network**: 10 Gbps для загрузки моделей vLLM

## 📦 Быстрая установка

### Автоматическая установка

```bash
# Клонирование проекта
git clone https://github.com/Eugene-SN/pdf-converter
cd pdf-converter

# Запуск полной автоматической установки
chmod +x full-install.sh
sudo ./full-install.sh
```

Скрипт автоматически:
- ✅ Проверяет системные требования
- ✅ Создаёт структуру проекта и каталоги ввода/вывода
- ✅ Размещает конфигурацию и ключи
- ✅ Загружает модели vLLM
- ✅ Собирает Docker-образы
- ✅ Запускает сервисы и проверяет их работоспособность

### Ручная установка

<details>
<summary>Пошаговые инструкции</summary>

```bash
# 1. Базовая директория
mkdir -p /mnt/storage/docker/pdf-converter
cd /mnt/storage/docker/pdf-converter

# 2. Создание структуры
mkdir -p flask airflow/dags vllm document_processor quality_assurance translator \
         pandoc/templates diff-pdf grafana/prometheus grafana/provisioning/{dashboards,datasources} prometheus
mkdir -p config logs plugins temp input_pdf output_md_{en,ru,zh} models/{huggingface,shared}

# 3. Размещение файлов — см. docs/deployment-guide.md

# 4. Генерация секретов
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
FLASK_SECRET=$(openssl rand -hex 32)

# 5. Обновление .env
sed -i "s/your_super_secret_key_here_change_in_production/$FLASK_SECRET/g" .env
sed -i "s/your_fernet_key_here_change_in_production/$FERNET_KEY/g" .env

# 6. Права доступа
chown -R 1000:1000 /mnt/storage/docker/pdf-converter
chmod -R 755 /mnt/storage/docker/pdf-converter

# 7. Сборка и запуск
docker compose build
docker compose up -d
```
</details>

## 🌐 Веб-интерфейсы

| Сервис | URL | Доступ | Описание |
|--------|-----|--------|----------|
| Flask API | http://localhost:5000 | токен/ключ из `.env` | Загрузка PDF и управление заданиями |
| Airflow UI | http://localhost:8080 | admin/admin | Настройка и мониторинг DAG |
| Grafana | http://localhost:3000 | admin/admin | Дашборды производительности и качества |
| Prometheus | http://localhost:9090 | — | Метрики системы |

## 🔧 Использование системы

### API и пакетная обработка

```bash
# Базовая конвертация
curl -X POST \
  -F "file=@document.pdf" \
  -F "target_language=en" \
  -F "quality_level=high" \
  http://localhost:5000/api/v1/convert

# Пакетная обработка
curl -X POST \
  -F "files=@document1.pdf" \
  -F "files=@document2.pdf" \
  -F "target_language=ru" \
  -F "batch_mode=true" \
  http://localhost:5000/api/v1/batch_convert
```

### Веб-интерфейс
1. Перейдите на http://localhost:5000
2. Загрузите PDF-файлы
3. Укажите целевой язык (en/ru/zh/original)
4. Выберите режим качества (high/medium/fast)
5. Нажмите «Конвертировать» и отслеживайте прогресс

### Мониторинг обработки
- **Airflow UI** — контроль DAG и задач
- **Grafana** — визуализация KPI и QA-метрик
- **Логи** — `docker compose logs -f <service>`

## 📊 Мониторинг и метрики

**Grafana** отслеживает:
- Pipeline Success Rate
- Processing Time by DAG
- Quality Assurance Scores
- vLLM Performance (latency, throughput)
- System Resources (CPU, RAM, GPU)

**Alerts** (Prometheus/Grafana):
- Ошибки в любом DAG
- Качество ниже 95 %
- Превышение SLA по времени
- Аномалии GPU/моделей

## 🔍 Устранение неполадок

### vLLM не запускается
```bash
nvidia-smi
nvcc --version
docker compose logs vllm
```

### Airflow не видит DAG
```bash
ls -la airflow/dags/
docker compose restart airflow-webserver airflow-scheduler
```

### Проблемы с правами доступа
```bash
sudo chown -R 1000:1000 /mnt/storage/docker/pdf-converter
sudo chmod -R 755 /mnt/storage/docker/pdf-converter
```

### Недостаточно памяти
```bash
docker stats
docker system prune -f
```

## 🗂️ Структура выходных файлов

```
output_md_{язык}/
└── [timestamp]_[документ]/
    ├── document.md         # Markdown
    ├── images/             # Изображения Docling/OCR
    │   ├── image_001.png
    │   └── image_002.png
    ├── metadata.json       # Метаданные конвертации
    └── qa_report.json      # Отчёт QA (5 уровней)
```

## 🔄 Управление и масштабирование

```bash
# Статус сервисов
docker compose ps

# Остановка
docker compose down

# Перезапуск
docker compose restart

# Логи
docker compose logs -f <service>

# Обновление
git pull
docker compose build
docker compose up -d

# Масштабирование воркеров
docker compose up -d --scale document-processor=3

docker compose up -d --scale quality-assurance=2
```

## 🛡️ Безопасность

- Работа под непривилегированным UID 1000
- Генерация секретов и ключей при установке
- Отсутствие чувствительных данных в репозитории
- Изоляция через Docker networks
- Централизованное логирование операций

## 🚀 Производительность

| Метрика | Значение | Условия |
|---------|----------|---------|
| Пропускная способность | 50–100 страниц/мин | 2× A6000, high-quality режим |
| Время обработки | 2–5 мин/документ | 10–20 страниц |
| Качество OCR | 95–99 % | Чёткие PDF |
| Качество перевода | 90–95 % | Технические документы |
| Общий балл QA | 95–100 % | После 5 уровней QA |

**Оптимизация:** пакетная обработка, настройка vLLM под GPU, мониторинг узких мест, горизонтальное масштабирование.

## 📚 Дополнительная документация

- Deployment guide, архитектура, API-описание и гайды по мониторингу доступны в каталоге `docs/` и проектной Wiki.

## 🤝 Вклад в проект

1. Fork репозитория
2. Создайте ветку: `git checkout -b feature/amazing-feature`
3. Коммит: `git commit -m "Add amazing feature"`
4. Push: `git push origin feature/amazing-feature`
5. Pull Request

## 📄 Лицензия

Проект распространяется под MIT License (см. `LICENSE`).

## 🆘 Поддержка

- Email: support@pdf-converter.local
- Issues: GitHub Issues
- Wiki: Проектная Wiki

## 🎯 Roadmap

### v2.1 (в разработке)
- Поддержка DOCX/PPTX
- Интеграция с облачными хранилищами
- REST API v2 с расширенными маршрутами
- ML-модели для автоулучшения качества

### v3.0 (план)
- Веб-интерфейс управления
- Дополнительные языки
- Кластерное развёртывание
- Специализированные AI-модели для отраслевых документов

**PDF Converter Pipeline v2.0** — готовое решение для высококачественной конвертации и перевода PDF-документов с применением современных технологий ИИ и глубокой валидацией. 🚀
