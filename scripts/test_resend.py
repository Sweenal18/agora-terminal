import requests

RESEND_API_KEY = "re_d5YKQTBr_MauCmbp7csTU4QEzJL48dUc8"

res = requests.post(
    "https://api.resend.com/emails",
    headers={
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json",
    },
    json={
        "from": "alerts@agora-terminal.com",
        "to": ["sweenalbandodcar1@gmail.com"],
        "subject": "Test from Agora Terminal",
        "html": "<p>Pipeline alerting is working.</p>"
    },
    timeout=15
)
print(f"Status: {res.status_code}")
print(f"Response: {res.text}")