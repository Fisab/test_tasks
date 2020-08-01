Описание тестовое задание находится в этой папке с названием файла: `Back_end_Постановка задачи.pdf`

Все тестовое задание реализовано в тетрадке под названием `main.ipynb`

Так же еще есть скрипт `main.py`, который выполняет тот же функционал и поддерживает кэширование, чтобы постоянно не скачивать файл из сети. В случае, если кэша на определенный файл нету, то скрипт попытается получить его из сети

```python
python3 main.py -h
usage: main.py [-h] -a ADDRESS [-c VIA_CACHE]

Script for calculate quality of life by your addressSupport generate and use
cache

optional arguments:
  -h, --help            show this help message and exit
  -a ADDRESS, --address ADDRESS
                        For this address script will calculate quality of life. If it contains more informat like ("Домодедовская, 42") please write this arg in quotas
  -c VIA_CACHE, --via_cache VIA_CACHE
                        Use cache? If yes and cache doesnt exists script try
                        to access file from source
```

