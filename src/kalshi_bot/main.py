from __future__ import annotations

import uvicorn

from kalshi_bot.config import get_settings
from kalshi_bot.logging import configure_logging
from kalshi_bot.web.app import create_app


def main() -> None:
    settings = get_settings()
    configure_logging()
    uvicorn.run(create_app(), host=settings.app_host, port=settings.app_port)


if __name__ == "__main__":
    main()

