import sys
import requests
import os
from datetime import datetime

RESEND_API_KEY = "re_d5YKQTBr_MauCmbp7csTU4QEzJL48dUc8"
FROM_EMAIL = "alerts@agora-terminal.com"
TO_EMAIL = "sweenalbandodcar1@gmail.com"

def send_alert(subject, body):
    res = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": FROM_EMAIL,
            "to": [TO_EMAIL],
            "subject": f"[Agora] {subject}",
            "html": f"""
            <div style="font-family: monospace; background: #0a0a0a; color: #00ff88; padding: 24px; border-radius: 8px;">
                <h2 style="color: #00ff88; margin-top: 0;">Agora Terminal Pipeline Alert</h2>
                <pre style="color: #ffffff; background: #1a1a1a; padding: 16px; border-radius: 4px; white-space: pre-wrap;">{body}</pre>
                <p style="color: #666; font-size: 12px; margin-top: 16px;">
                    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST &mdash;
                    <a href="https://agora-terminal.com" style="color: #00ff88;">agora-terminal.com</a>
                </p>
            </div>
            """
        },
        timeout=15
    )
    if res.status_code == 200:
        print(f"Alert sent: {subject}")
        return True
    else:
        print(f"Alert failed: {res.status_code} {res.text}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: alert.py <subject> <body>")
        sys.exit(1)
    send_alert(sys.argv[1], sys.argv[2])