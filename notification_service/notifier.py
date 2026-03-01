import smtplib
from email.mime.text import MIMEText
from config import EMAIL_HOST, EMAIL_PORT, EMAIL_USER, EMAIL_PASSWORD
from datetime import datetime, timezone, timedelta


def get_datestring_UNIX_to_IST(t) -> str:
    IST = timezone(timedelta(hours=5, minutes=30))
    return datetime.fromtimestamp(t, tz=IST).strftime('%Y-%m-%d %H:%M:%S')

def send_email(alert_payload, recipients):

    subject = f"ALERT: Machine {alert_payload['machine_id']}"

    body = f"""
Alarm Triggered

Machine ID: {alert_payload['machine_id']}
Severity: {alert_payload.get('severity')}
Confidence Score: {alert_payload.get('confidence_score')}
String Timestamp: {get_datestring_UNIX_to_IST(alert_payload.get('timestamp'))}
Integer Timestamp: {alert_payload.get('timestamp')}
"""

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.sendmail(
            from_addr=EMAIL_USER,
            to_addrs=recipients,
            msg=msg.as_string()
        )