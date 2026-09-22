# Polymarket Orderbook Ingest (ClickHouse)

Текущая версия проекта делает сбор данных для стакана и отдельного `Current price` потока (без полноценной аналитики Black-Scholes в runtime).

## Что делает `ws_ingest.py`

Скрипт:
- находит Polymarket crypto markets через Gamma API,
- выбирает текущий/ближайший `15m` рынок отдельно для каждого символа из `POLY_SYMBOLS`,
- подписывается на Market WebSocket,
- сохраняет в ClickHouse:
  - `orderbook` — один стартовый snapshot стакана (UP) на рынок,
  - `pricechange` — последующие изменения `size` по уровням стакана (дельты).

Для multi-crypto режима используйте:
- `POLY_SYMBOLS=BTC,ETH,SOL,XRP`
- `POLY_TIMEFRAMES=15m`
- `POLY_TRACK_CURRENT_ONLY=true`
- `POLY_MAX_MATCHED_MARKETS=4`

## Что делает `spot_ingest.py`

Скрипт:
- подписывается на `ws-live-data.polymarket.com` (RTDS),
- читает поток `Current price` (`topic=crypto_prices_chainlink`) сразу по нескольким символам из `SPOT_SYMBOLS`,
- сохраняет тики в ClickHouse таблицу `crypto_spot` (или `SPOT_TABLE` из `.env`).

Для RTDS используется отдельное websocket-соединение на каждый `symbol`. Это намеренно: несколько подписок на один topic в рамках одного соединения могут перетирать друг друга на стороне сервера.

Этот поток запускается независимо от `ws_ingest.py`, чтобы `price_to_beat` можно было быстро брать на границе нового рынка без задержки market reconnect.

## Таблицы ClickHouse

### `orderbook`
- один snapshot на `(market, asset_id)` для UP-стороны выбранного рынка
- фиксированная сетка цен `0.001..0.999` с шагом `0.001`
- ровно `999` строк на snapshot
- если уровня нет в пришедшем book-сообщении, пишется `size = 0`

### `pricechange`
- не сделки и не trade prints
- это изменения размера уровня стакана:
  - `size > 0` — объем добавили
  - `size < 0` — объем сняли/уменьшили

### `ingest_heartbeat` (служебная)
- периодический статус ingest-процесса
- помогает понять: жив ли процесс, какой рынок сейчас отслеживается, какой лаг по времени

### `ingest_service_log` (служебная)
- сервисные события ingest:
  - старт,
  - смена рынка (refresh / reload),
  - reconnect,
  - ошибки WS / ClickHouse insert

### `crypto_spot`
- тики `Current price` из RTDS
- ключевые поля:
  - `symbol` (`btc/usd`, `eth/usd`, `sol/usd`, `xrp/usd`, ...)
  - `event_ts_ms` (время цены)
  - `value` (цена)
  - `full_accuracy_value` (строковое high-precision значение, если есть)

## Автопереход на следующий рынок

Если `POLY_MARKET_WHITELIST_IDS` пустой и включен:
- `POLY_TRACK_CURRENT_ONLY=true`
- `POLY_MAX_MATCHED_MARKETS=1`

тогда ingest отслеживает только текущий рынок и при обновлении discovery переключается на следующий временной слот.

## Файлы проекта

- `ws_ingest.py` — основной ingest
- `spot_ingest.py` — ingest `Current price` из RTDS в `crypto_spot`
- `jupyter_clickhouse_workflow.py` — набор готовых Jupyter-команд для проверки данных
- `.env.example` — шаблон конфигурации
- `requirements.txt` — зависимости

## Быстрый старт

1. Создай `.env` из `.env.example`
2. Заполни `CLICKHOUSE_PASSWORD`
3. Установи зависимости:
   - `pip install -r requirements.txt`
4. Запусти ingest стакана:
   - `python ws_ingest.py`
5. В отдельном процессе запусти spot ingest:
   - `python spot_ingest.py`

## Проверка в Jupyter

Используй `jupyter_clickhouse_workflow.py`:
- подключение к ClickHouse (`ch_cmd`, `ch_df`)
- очистка таблиц
- список snapshot'ов
- вывод полного snapshot'а и ненулевых уровней
- вывод `pricechange`

Для `price_to_beat`:
- бери первый тик из `crypto_spot` на/после старта нового 15m рынка,
- если такого тика нет, можно брать последний тик до старта как fallback.

## Практическая заметка

Если при выводе snapshot'а в Jupyter кажется, что все `size = 0`, это часто из-за превью DataFrame:
- Jupyter показывает только верх/низ таблицы,
- а ненулевые уровни обычно находятся в середине диапазона цен.

## Надежность ingest (MVP)

В текущей версии добавлены:
- heartbeat в ClickHouse (`ingest_heartbeat`)
- сервисный лог (`ingest_service_log`)

Это не влияет на бизнес-данные (`orderbook` / `pricechange`), но помогает быстро диагностировать зависания, ошибки и ротацию рынков.
