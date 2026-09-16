import os

import uvicorn
from fastapi import FastAPI

from .deps import get_settings

settings = get_settings()

app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "ok", "settings_imported": settings["service"]}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        workers=2,
    )
