def handler(event, context):
    user = event["newImage"]

    print(f"User updated: {user['id']}")
