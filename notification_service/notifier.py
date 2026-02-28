import smtplib
from email.mime.text import MIMEText
from config import EMAIL_HOST, EMAIL_PORT, EMAIL_USER, EMAIL_PASSWORD

def send_email(alert_payload):

    subject = f"ALERT: Machine {alert_payload['machine_id']}"

    body = f"""
    Alarm Triggered

    Machine ID: {alert_payload['machine_id']}
    Severity: {alert_payload['severity']}
    Confidence Score: {alert_payload['confidence_score']}
    Timestamp: {alert_payload['timestamp']}
    """

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_USER

    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_USER, [EMAIL_USER], msg.as_string())