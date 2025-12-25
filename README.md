# MAI-IR Search Engine

Учебная поисковая система (ЛР 1-7).
Реализация ядра (индексация, поиск, сжатие) выполнена на **C++17** без использования STL контейнеров (собственные `SimpleVector`, `HashMap`). Обвязка и краулер на Python.

## Структура
*   `bin/` — Скомпилированные бинарники (tokenizer, indexer, search).
*   `src/` — Исходный код C++ (ядро системы).
*   `search_engine/` — Python модули (crawler, utils).
*   `index/` — Бинарные файлы индекса.

## Установка

Требуется: Python 3.8+, g++ (C++17).

```bash
# Автоматическая сборка (создает venv, ставит зависимости, компилирует C++)
chmod +x setup.sh
./setup.sh

# Активация окружения
source venv/bin/activate
```

## Использование

### 1. Сбор данных (Crawler)
Скачивает документы из Web (Biorxiv) или NCBI API.
```bash
python3 run_crawler.py --config config.yaml
```

### 2. Индексация (C++ Core)
Читает данные из MongoDB, токенизирует, строит обратный индекс (SPIMI), сжимает (VarByte) и сохраняет в `index/`.
```bash
python3 run_indexer.py --config config.yaml
```

### 3. Поиск (CLI)
Поддерживает интерактивный режим и пакетную обработку.
```bash
# Интерактивно
python3 run_search.py

# Из файла (Batch)
python3 run_search.py --input-file queries.txt --output-file results.txt

# Unix Pipe
echo "gene editing" | python3 run_search.py
```

### 4. Поиск (Web UI)
Запускает локальный веб-сервер.
```bash
python3 app.py
# Открыть http://127.0.0.1:5000
```

### 5. Анализ и Бенчмарки
```bash
# Закон Ципфа
python3 run_analysis.py --config config.yaml

# Замер скорости токенизации и поиска
python3 run_benchmark.py
```
