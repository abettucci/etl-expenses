#!/usr/bin/env python3
"""Send an explicit test notification to the configured variation-alert channels."""

import argparse
import os
import sys

DEFAULT_MESSAGE = "🧪 Prueba de alerta de variación de gastos. Si recibiste este mensaje, el canal funciona correctamente."
ALLOWED_CHANNELS = {"telegram", "sns", "both"}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Prueba controlada de Telegram y SNS")
    parser.add_argument("--channel", choices=sorted(ALLOWED_CHANNELS), default="both")
    parser.add_argument("--message", default=DEFAULT_MESSAGE)
    parser.add_argument("--subject", default="Prueba de alerta de gastos")
    parser.add_argument("--send", action="store_true", help="Envía realmente; sin este flag sólo valida configuración")
    args = parser.parse_args(argv)
    if not 1 <= len(args.message) <= 1000:
        parser.error("--message debe tener entre 1 y 1000 caracteres")
    if not 1 <= len(args.subject) <= 100:
        parser.error("--subject debe tener entre 1 y 100 caracteres")
    return args


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Falta configurar {name}")
    return value


def validate_configuration(channel: str) -> None:
    if channel in {"telegram", "both"}:
        required_env("TELEGRAM_BOT_TOKEN")
        required_env("TELEGRAM_ALERT_CHAT_ID")
    if channel in {"sns", "both"}:
        topic_arn = required_env("ALERT_SNS_TOPIC_ARN")
        if not topic_arn.startswith("arn:aws:sns:"):
            raise ValueError("ALERT_SNS_TOPIC_ARN no es un ARN SNS válido")


def send_telegram(message: str) -> None:
    import requests

    token = required_env("TELEGRAM_BOT_TOKEN")
    chat_id = required_env("TELEGRAM_ALERT_CHAT_ID")
    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": message},
        timeout=10,
        allow_redirects=False,
    )
    response.raise_for_status()
    if not response.json().get("ok"):
        raise RuntimeError("Telegram rechazó la notificación")


def send_sns(subject: str, message: str) -> None:
    import boto3

    boto3.client("sns").publish(
        TopicArn=required_env("ALERT_SNS_TOPIC_ARN"),
        Subject=subject,
        Message=message,
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    try:
        validate_configuration(args.channel)
        if not args.send:
            print(f"Dry-run OK: configuración válida para {args.channel}. Repetí con --send para notificar.")
            return 0
        if args.channel in {"telegram", "both"}:
            send_telegram(args.message)
            print("Telegram: notificación enviada")
        if args.channel in {"sns", "both"}:
            send_sns(args.subject, args.message)
            print("SNS: notificación enviada")
        return 0
    except Exception as exc:
        print(f"Prueba de notificación falló: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
