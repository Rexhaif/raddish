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
import argparse as ap

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


async def main(fn_name: str = "add_one") -> None:
    redis_client: Redis = Redis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379")
    )
    client: AsyncRaddishClient = AsyncRaddishClient(redis_client)

    counter: int = 0
    for _ in range(5):
        logger.info(f"Sending input {counter}")
        counter = (await client.call(fn_name, {"number": counter}))["number"]
        logger.info(f"Received output {counter}")


if __name__ == "__main__":
    parser = ap.ArgumentParser()
    parser.add_argument(
        "--fn-name",
        type=str,
        default="add_one",
        help="Name of the function to call",
    )
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args.fn_name))
