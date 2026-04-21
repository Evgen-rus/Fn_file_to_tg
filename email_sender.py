import mimetypes
import logging
import os
import smtplib
import time
from email.message import EmailMessage
from typing import List, Tuple


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Не задана переменная окружения: {name}")
    return value


def parse_email_recipients(raw_value: str) -> List[str]:
    recipients = [
        item.strip()
        for item in raw_value.replace(";", ",").split(",")
        if item.strip()
    ]
    if not recipients:
        raise ValueError("Не задан ни один получатель письма.")
    return recipients


def _get_email_provider() -> str:
    """Возвращает 'unisender' или 'yandex' (без учёта регистра)."""
    provider = (os.getenv("EMAIL_PROVIDER") or "unisender").strip().lower()
    if provider not in ("unisender", "yandex"):
        return "unisender"
    return provider


def load_email_config_from_env() -> dict:
    provider = _get_email_provider()
    to_emails = parse_email_recipients(get_required_env("UNIS_TO_EMAIL"))

    if provider == "yandex":
        return {
            "smtp_host": get_required_env("SMTP_SERVER"),
            "smtp_port": int(get_required_env("SMTP_PORT")),
            "smtp_username": get_required_env("YANDEX_EMAIL"),
            "smtp_password": get_required_env("YANDEX_APP_PASSWORD"),
            "from_email": get_required_env("YANDEX_EMAIL"),
            "to_emails": to_emails,
            "use_ssl": True,
        }
    # unisender
    return {
        "smtp_host": get_required_env("UNIS_SMTP_HOST"),
        "smtp_port": int(get_required_env("UNIS_SMTP_PORT")),
        "smtp_username": get_required_env("UNIS_SMTP_USERNAME"),
        "smtp_password": get_required_env("UNIS_SMTP_PASSWORD"),
        "from_email": get_required_env("UNIS_FROM_EMAIL"),
        "to_emails": to_emails,
        "use_ssl": False,
    }


def _humanize_email_error(error: Exception) -> str:
    text = str(error).lower()

    if isinstance(error, FileNotFoundError):
        return "файл для отправки не найден"
    if "не задана переменная окружения" in text:
        return "не заполнены настройки почты в .env"
    if "authentication" in text or "auth" in text or "535" in text:
        return "неверный логин или пароль SMTP"
    if "sender address rejected" in text:
        return "адрес отправителя не разрешён на стороне почтового сервиса"
    if "recipient address rejected" in text:
        return "адрес получателя отклонён почтовым сервисом"
    if "starttls" in text or "tls" in text:
        return "ошибка защищённого подключения к SMTP-серверу"
    if "timed out" in text or "timeout" in text:
        return "SMTP-сервер не ответил вовремя"
    if "connection refused" in text or "server disconnected" in text:
        return "SMTP-сервер недоступен или разорвал соединение"

    return "внутренняя ошибка отправки письма"


def _build_message(
    from_email: str,
    to_emails: List[str],
    subject: str,
    body: str,
    attachment_path: str,
) -> EmailMessage:
    if not os.path.exists(attachment_path):
        raise FileNotFoundError(f"Файл для письма не найден: {attachment_path}")

    message = EmailMessage()
    message["From"] = from_email
    message["To"] = ", ".join(to_emails)
    message["Subject"] = subject
    message.set_content(body)

    mime_type, _ = mimetypes.guess_type(attachment_path)
    if mime_type is None:
        mime_type = "application/octet-stream"
    maintype, subtype = mime_type.split("/", 1)

    with open(attachment_path, "rb") as file:
        message.add_attachment(
            file.read(),
            maintype=maintype,
            subtype=subtype,
            filename=os.path.basename(attachment_path),
        )

    return message


def send_email_with_attachment(
    smtp_host: str,
    smtp_port: int,
    smtp_username: str,
    smtp_password: str,
    from_email: str,
    to_emails: List[str],
    subject: str,
    body: str,
    attachment_path: str,
    use_ssl: bool = False,
) -> None:
    message = _build_message(
        from_email=from_email,
        to_emails=to_emails,
        subject=subject,
        body=body,
        attachment_path=attachment_path,
    )

    if use_ssl:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
            server.login(smtp_username, smtp_password)
            server.send_message(message)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_username, smtp_password)
            server.send_message(message)


def send_email_with_attachment_with_retries(
    subject: str,
    body: str,
    attachment_path: str,
    max_retries: int = 5,
    base_delay_sec: float = 2.0,
) -> Tuple[bool, str]:
    try:
        config = load_email_config_from_env()
    except Exception as error:
        return False, f"не заполнены настройки почты: {error}"

    last_error_human = "неизвестная ошибка"
    last_error_technical = ""

    for attempt in range(1, max_retries + 1):
        try:
            send_email_with_attachment(
                smtp_host=config["smtp_host"],
                smtp_port=config["smtp_port"],
                smtp_username=config["smtp_username"],
                smtp_password=config["smtp_password"],
                from_email=config["from_email"],
                to_emails=config["to_emails"],
                subject=subject,
                body=body,
                attachment_path=attachment_path,
                use_ssl=config.get("use_ssl", False),
            )
            recipients = ", ".join(config["to_emails"])
            return True, f"Письмо на почту отправлено: {recipients}."
        except Exception as error:
            last_error_human = _humanize_email_error(error)
            last_error_technical = str(error)
            logging.getLogger(__name__).warning(
                "Ошибка отправки email, попытка %s/%s: %s",
                attempt,
                max_retries,
                last_error_technical,
            )
            if attempt < max_retries:
                delay = base_delay_sec * (2 ** (attempt - 1))
                time.sleep(delay)

    return False, f"не удалось отправить письмо после {max_retries} попыток: {last_error_human}"
