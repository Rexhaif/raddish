# Copyright 2023 Daniil Larionov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================


import asyncio
import logging
import os

from redis.asyncio import Redis
from rich.logging import RichHandler

from raddish import AsyncWorker, RaddishServer

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)],
)

logger = logging.getLogger("raddish-test")


class TestWorkerAddOne(AsyncWorker):
    async def setup(self) -> None:
        logger.info("Setting up test worker")

    async def __call__(self, input: dict[str, any]) -> dict[str, any]:
        input: int = input["number"]
        input += 1
        await asyncio.sleep(10)
        return {
            "number": input,
        }
    
class TestWorkerPower2(AsyncWorker):
    async def setup(self) -> None:
        logger.info("Setting up test worker")

    async def __call__(self, input: dict[str, any]) -> dict[str, any]:
        input: int = input["number"]
        input = input ** 2
        await asyncio.sleep(10)
        return {
            "number": input,
        }


async def main() -> None:
    redis_client: Redis = Redis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379")
    )
    server: RaddishServer = RaddishServer(redis_client)
    server.add_worker("add_one", TestWorkerAddOne())
    server.add_worker("power_2", TestWorkerPower2())
    await server.start()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
