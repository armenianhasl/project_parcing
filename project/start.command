#!/bin/bash
set -e
cd -- "$(dirname -- "$0")"
if [ ! -x .venv/bin/python ]; then
    echo "Не найдено окружение .venv. Создайте его по инструкции в README.md."
    exit 1
fi
exec .venv/bin/python project
