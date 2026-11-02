def handler(event):
    user = event.get("oldImage", {})

    print(f"User deleted: {user.get('id', 'unknown')}")
