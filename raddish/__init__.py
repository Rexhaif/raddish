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

import abc
import asyncio
import datetime
import logging
import platform
import signal
import time
import uuid

import orjson as json
from redis.asyncio import Redis

__version__ = "0.1.0"

HOSTNAME: str = platform.node()
WORKER_NAME: str = f"{HOSTNAME}-{str(uuid.uuid4())}"


class Worker(abc.ABC):
    @abc.abstractmethod
    async def setup(self) -> None:
        pass

    @abc.abstractmethod
    async def __call__(self, input: dict[str, any]) -> dict[str, any]:
        pass


class RaddishServer:
    def __init__(self, redis_client: Redis, key_prefix: str = "raddish") -> None:
        self.redis_client: Redis = redis_client
        self.key_prefix: str = key_prefix
        self.logger: logging.Logger = logging.getLogger("raddish-server")
        self.workers: dict[str, Worker] = {}
        self.loop_fns: list[asyncio.Task] = []

        self.logger.info("Registering exit handler")
        for signum, sigstr in {(signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")}:
            asyncio.get_event_loop().add_signal_handler(
                signum, lambda: asyncio.create_task(self.stop(f"{sigstr} received"))
            )

    async def __announce_worker(self, function_name: str) -> None:
        worker_data = {"worker": HOSTNAME, "joined_at": datetime.datetime.now()}

        current_fn_data: dict[str, dict] = {}

        if await self.redis_client.hexists(
            f"{self.key_prefix}/functions", function_name
        ):
            self.logger.info(f"Updating function {function_name}")
            current_fn_data: dict[str, dict] = json.loads(
                await self.redis_client.hget(
                    f"{self.key_prefix}/functions", function_name
                )
            )

        current_fn_data[WORKER_NAME] = worker_data

        await self.redis_client.hset(
            f"{self.key_prefix}/functions",
            function_name,
            json.dumps(current_fn_data),
        )
        self.logger.info(f"Announced worker {WORKER_NAME} for function {function_name}")

    async def __deannounce_worker(self, function_name: str) -> None:
        self.logger.info(
            f"Deannouncing worker {WORKER_NAME} for function {function_name}"
        )
        current_fn_data: dict[str, dict] = json.loads(
            await self.redis_client.hget(f"{self.key_prefix}/functions", function_name)
        )

        del current_fn_data[WORKER_NAME]

        if len(current_fn_data) == 0:
            await self.redis_client.hdel(f"{self.key_prefix}/functions", function_name)
        else:
            await self.redis_client.hset(
                f"{self.key_prefix}/functions",
                function_name,
                json.dumps(current_fn_data),
            )
        self.logger.info(
            f"Deannounced worker {WORKER_NAME} for function {function_name}"
        )

    def add_worker(self, function_name: str, worker: Worker) -> None:
        self.workers[function_name] = worker

    async def make_loop_fn(self, function_name: str, worker: Worker) -> None:
        async def loop_fn() -> None:
            logger = logging.getLogger(f"raddish-server.{function_name}")
            logger.info(f"Setting up worker for function {function_name}")
            _t0 = time.monotonic()
            await worker.setup()
            _t1 = time.monotonic()
            logger.info(f"Setup for {function_name} took {_t1 - _t0} seconds")

            input_counter: int = 0
            while True:
                try:
                    logger.info(
                        f"Waiting for input #{input_counter} for function {function_name}"
                    )
                    _t0 = time.monotonic()
                    fn_input: bytes = (
                        await self.redis_client.blpop(
                            [f"{self.key_prefix}/{function_name}/input"]
                        )
                    )[1]
                    _t1 = time.monotonic()
                    logger.info(
                        f"Received input #{input_counter} for function {function_name} after {_t1 - _t0} seconds"
                    )
                    logger.info(f"Processing input for function {function_name}")
                    logger.info(f"Input: {fn_input}")
                    _t0 = time.monotonic()
                    fn_input: dict[str, any] = json.loads(fn_input)
                    req_id: str = fn_input["req_id"]
                    try:
                        fn_output: any = await worker(fn_input["input"])
                    except Exception as e:
                        logger.error(
                            f"Error processing input #{input_counter} for function {function_name}"
                        )
                        logger.error(e)
                        fn_output: any = {"error": str(e)}
                    _t1 = time.monotonic()
                    logger.info(
                        f"Processed input #{input_counter} for function {function_name} in {_t1 - _t0} seconds"
                    )
                    logger.info(
                        f"Sending output #{input_counter} for function {function_name}"
                    )
                    fn_output: dict[str, any] = {
                        "ready_at": datetime.datetime.now(),
                        "worker": WORKER_NAME,
                        "processing_time": _t1 - _t0,
                        "output": fn_output,
                    }
                    fn_output: bytes = json.dumps(fn_output)
                    logger.info(
                        f"Output #{input_counter} for function {function_name} size: {len(fn_output)} bytes"
                    )
                    await self.redis_client.lpush(
                        f"{self.key_prefix}/{function_name}/output/{req_id}", fn_output
                    )
                except asyncio.CancelledError:
                    logger.info(f"Worker for function {function_name} cancelled")
                    await self.__deannounce_worker(function_name)
                    break

        return loop_fn

    async def start(self) -> None:
        for function_name, worker in self.workers.items():
            loop_fn = await self.make_loop_fn(function_name, worker)
            self.logger.info(f"Starting worker for function {function_name}")
            await self.__announce_worker(function_name)
            self.loop_fns.append(asyncio.create_task(loop_fn(), name=function_name))

        self.logger.info(
            f"Started all {len(self.loop_fns)} workers, waiting for them to finish"
        )
        await asyncio.gather(*self.loop_fns)

    async def stop(self, reason: str = "unknown") -> None:
        self.logger.info(f"Stop invoked with reason: {reason}")
        self.logger.info(f"Stopping all {len(self.loop_fns)} workers")
        for loop_fn in self.loop_fns:
            loop_fn.cancel()
        self.logger.info(f"Stopped all {len(self.loop_fns)} workers")

        self.logger.info("All workers stopped")


class RaddishClient:
    def __init__(self, redis_client: Redis, key_prefix: str = "raddish") -> None:
        self.redis_client: Redis = redis_client
        self.key_prefix: str = key_prefix
        self.logger: logging.Logger = logging.getLogger("raddish-client")

    async def __call__(
        self, function_name: str, input: dict[str, any], timeout: int = 60
    ) -> any:
        req_id: str = str(uuid.uuid4())
        fn_input: dict[str, any] = {
            "req_id": req_id,
            "input": input,
        }
        fn_input: bytes = json.dumps(fn_input)
        self.logger.info(f"Sending input for function {function_name}")
        await self.redis_client.lpush(
            f"{self.key_prefix}/{function_name}/input", fn_input
        )
        self.logger.info(f"Waiting for output for function {function_name}")
        fn_output: bytes = (
            await self.redis_client.blpop(
                [f"{self.key_prefix}/{function_name}/output/{req_id}"], timeout=timeout
            )
        )[1]
        self.logger.info(f"Received output for function {function_name}")
        fn_output: dict[str, any] = json.loads(fn_output)
        self.logger.info(
            f"Output for function {function_name} was ready at {fn_output['ready_at']}, processed by {fn_output['worker']} in {fn_output['processing_time']} seconds"
        )
        return fn_output["output"]
