"""Lightweight email helper for weekly digest with inline PNG charts."""
from __future__ import annotations
import smtplib
import ssl
from email.message import EmailMessage
from typing import Sequence

def send_email(subject: str, html_body: str, recipients: Sequence[str], sender: str, smtp_host: str, smtp_port: int = 587, username: str | None = None, password: str | None = None, attachments: list[tuple[str, bytes, str]] | None = None) -> dict:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content("This email requires an HTML capable client.")
    msg.add_alternative(html_body, subtype="html")
    for att in attachments or []:
        filename, data, mime = att
        maintype, subtype = mime.split("/",1)
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
            server.starttls(context=context)
            if username and password:
                server.login(username, password)
            server.send_message(msg)
        return {"status": "sent"}
    except Exception as e:  # noqa
        return {"status": "error", "error": str(e)}

def inline_img_cid(name: str) -> str:
    return f"cid:{name}"
