def handler(event):
    user = event["newImage"]

    print(f"User updated: {user['id']}")
