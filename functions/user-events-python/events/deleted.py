def handler(event):
    user = event.get("old_image", {})

    print(f"User deleted: {user.get('id', 'unknown')}")
