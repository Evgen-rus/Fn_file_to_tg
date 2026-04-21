import os
import sys

from dotenv import load_dotenv
from email_sender import load_email_config_from_env, send_email_with_attachment


def send_test_email() -> None:
    load_dotenv()
    provider = (os.getenv("EMAIL_PROVIDER") or "unisender").strip().lower()
    if provider != "unisender":
        print("Для теста UniSender поменяйте EMAIL_PROVIDER в .env на unisender.")
        sys.exit(0)

    attachment_path = os.getenv("TEST_EMAIL_ATTACHMENT_PATH")
    if not attachment_path:
        raise ValueError("Не задана переменная окружения TEST_EMAIL_ATTACHMENT_PATH")

    # Относительный путь — ищем в корне проекта
    if not os.path.isabs(attachment_path):
        project_root = os.path.dirname(os.path.abspath(__file__))
        attachment_path = os.path.join(project_root, attachment_path)

    config = load_email_config_from_env()
    send_email_with_attachment(
        smtp_host=config["smtp_host"],
        smtp_port=config["smtp_port"],
        smtp_username=config["smtp_username"],
        smtp_password=config["smtp_password"],
        from_email=config["from_email"],
        to_emails=config["to_emails"],
        subject="Тестовая отправка файла",
        body="Это тестовое письмо с вложением из проекта LeadRecord.",
        attachment_path=attachment_path,
        use_ssl=config.get("use_ssl", False),
    )


if __name__ == "__main__":
    try:
        send_test_email()
        print("Письмо успешно отправлено.")
    except Exception as error:
        print("Ошибка при отправке письма:")
        print(error)