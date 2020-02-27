# Тестовое задание на разработчика

Так звучало тестовое задание:

Исходные данные:

[golden_globe_awards.csv](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/45d7b67f-9fd6-4067-b957-6efc36b99ff1/golden_globe_awards.csv)

csv, данные по номинациям Золотой Глобус

 

**Задача:**

Написать скрипт загрузки данных в базу данных. Используя параметры запуска, предусмотреть:

- Очистку таблицы перед загрузкой данных
- Выбор даты какого года загружать
- Выбор, загружать всех или только победителей

Предусмотреть недоступность базы данных, исключить (обработать любым способом: выдать ошибку, перезаписать данные) дублирование записей.





### Скрипт для загрузки CSV файлов в базу данных

Как пользоваться скриптом:
```bash
$ python3 main.py -h
usage: main.py [-h] -f FILE_NAME [--batch_size BATCH_SIZE] [--purge_all_data]
               [--create_db] [-y YEAR_AWARD] [--only_winners] [--limit LIMIT]

Simple script to upload data from file_name to SQLite. Support uploading via
chunks. Existed rows at tables will be skiped

optional arguments:
  -h, --help            show this help message and exit
  -f FILE_NAME, --file_name FILE_NAME
                        From this file script retrieve data and load to table
                        with same name
  --batch_size BATCH_SIZE
                        Batch size for uploading data to database
  --purge_all_data      Remove all data from table before upload data
  --create_db           Will create database if it's doesn't exist
  -y YEAR_AWARD, --year_award YEAR_AWARD
                        Which year_award upload to database from file?
  --only_winners        Upload only winners to table
  --limit LIMIT         How many lines read? (from top)

```

Скрипт поддерживает два вида загрузки в базу данных - все данные, или чанками (пакетами). Таким образом есть возможность масштабировать входные данные и скрипт не будет спотыкаться об утилизацию ОЗУ.

Доступность базы данных SQLite проверяется наличием файла `database.db`, если его нету, то скрипт выбьет ошибку. Чтобы создать новую базу данных (при отсутствии `database.db`), нужно указать флаг `--create_db`, при запуске.

Файл будет записан в таблицу с названием файла (исключая его расширения, пример: `{file_name}.csv`, тогда название таблица - `{file_name}`).

---
После написания скрипта я вижу два варианта, как можно улучшить мое решение:
- Использовать `ORM`, а не генерировать запросы
- Испольвать `asyncio` при выполнении `SQL` запросов, чтобы не ожидать ответа базы данных и продолжать итерировать строки (в случае подхода с чанками)
