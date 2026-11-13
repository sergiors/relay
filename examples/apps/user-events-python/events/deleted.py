def handler(event, context):
    user = event.get("oldImage", {})

    print(f"User deleted: {user.get('id', 'unknown')}")
