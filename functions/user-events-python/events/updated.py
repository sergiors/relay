def handler(event):
    user = event["new_image"]

    print(f"User updated: {user['id']}")
