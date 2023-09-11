# Raddish

Raddish is mix of RPC and job queue backed by Redis. You can launch multiple servers that will accept the same function calls in a round-robin fashion. From the client side you can asynchronously submit jobs and block the execution until they are completed.

---
# Getting started

1. Implement a server
```python
import asyncio
import logging
import os

from redis.asyncio import Redis
from rich.logging import RichHandler

from raddish import RaddishServer, AsyncWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)

logger = logging.getLogger("raddish-test")


class TestWorker(AsyncWorker):
    async def setup(self) -> None:
        logger.info("Setting up test worker")

    async def __call__(self, input: dict[str, any]) -> dict[str, any]:
        logger.info(f"Received input: {input}")
        input: int = input["number"]
        input += 1
        await asyncio.sleep(10)
        return {
            "number": input,
        }


async def main() -> None:
    redis_client: Redis = Redis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379")
    )
    server: RaddishServer = RaddishServer(redis_client)
    server.add_worker("add_one", TestWorker())
    await server.start()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

```

2. Implement a client
```python
import asyncio
import logging
import os

from redis.asyncio import Redis
from rich.logging import RichHandler

from raddish import AsyncRaddishClient

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)

logger = logging.getLogger("raddish-test")


async def main() -> None:
    redis_client: Redis = Redis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379")
    )
    client: AsyncRaddishClient = AsyncRaddishClient(redis_client)

    counter: int = 0
    for _ in range(5):
        logger.info(f"Sending input {counter}")
        counter = (await client.call("add_one", {"number": counter}))["number"]
        logger.info(f"Received output {counter}")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

```

3. Launch one or more servers
```bash
python server.py
```

4. Launch one or more clients
```bash
python client.py
```

---
# Features

* You can add servers at any time in order to increase capacity
* Asyncio-native
* Servers can be located anywhere where there is internet, no need to have public ip - you only need to be able to connect to the redis server


---
# License

Raddish is licensed under Apache License, Version 2.0