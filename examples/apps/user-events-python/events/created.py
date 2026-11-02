import os

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")


def handler(event):
    user = event["newImage"]

    print(f"User created: {user['id']}")
    print(f"POSTGRES_DSN={POSTGRES_DSN}")
