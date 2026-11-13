import os

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")


def handler(event, context):
    user = event["newImage"]

    print(
        f"Request {context.request_id} has "
        f"{context.get_remaining_time_ms()}ms left"
    )
    print(f"User created: {user['id']}")
    print(f"POSTGRES_DSN={POSTGRES_DSN}")
