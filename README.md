# Сбор данных Polymarket

Актуальный код находится в [project/](project/README.md).
Сбор UP-стаканов BTC, ETH, SOL и XRP, цен Chainlink и TWAP в ClickHouse.

## Подготовка и запуск

Нужны Python 3.10+ на macOS/Linux и доступная база ClickHouse.

```sh
cd project
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp -n .env.example .env
```

Заполните настройки подключения и переключатели валют в `.env`, затем выполните:

```sh
.venv/bin/python project
```

Команда запускает оба процесса. Для остановки нажмите Ctrl+C и дождитесь завершения.
В облаке настройки можно задать переменными окружения без файла `.env`.
Для прогона на минуту: `COLLECTOR_RUN_SECONDS=60 .venv/bin/python project`.

Пароли, `.env`, резервные копии, кэш и локальная очередь `.collector` не публикуются.
