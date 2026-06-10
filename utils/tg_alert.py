"""
Модуль для отправки уведомлений в Telegram через Bot API.

Использует переменные окружения:
- TELEGRAM_BOT_TOKEN — токен бота Telegram.
- TELEGRAM_CHAT_ID — ID чата, куда отправлять сообщения.

Функция  отправляет текстовое сообщение в указанный чат,
с параметром silent для отключения звукового уведомления.

Обработка ошибок и отсутствие переменных окружения логируются, 
чтобы не прерывать основной процесс.
"""


import os
import logging
import requests

def alert_telegram(message: str, silent: bool = False) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logging.warning("Telegram: переменные окружения не заданы!")
        return

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": message,
                "disable_notification": silent
            },
            timeout=5
        )
        if response.status_code != 200:
            logging.warning(f"Telegram: ошибка {response.status_code} — {response.text}")

    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения в Telegram: {e}")

if __name__ == "__main__":
    pass