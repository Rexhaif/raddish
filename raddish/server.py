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
from typing import Any

from redis.asyncio import Redis

from .utils import unified_json_dumps, unified_json_loads


class AsyncWorker(abc.ABC):
    async def setup(self) -> None:
        """
        Called once before the worker starts processing inputs
        """
        pass

    @abc.abstractmethod
    async def __call__(self, input: dict[str, Any]) -> dict[str, Any]:
        pass

    async def shutdown(self) -> None:
        """
        Called once after the worker is shutting down
        """
        pass


class RaddishServer:
    def __init__(self, redis_client: Redis, key_prefix: str = "raddish") -> None:
        self.redis_client: Redis = redis_client
        self.key_prefix: str = key_prefix
        self.logger: logging.Logger = logging.getLogger("raddish-server")
        self.workers: dict[str, AsyncWorker] = {}
        self.loop_fns: list[asyncio.Task] = []
        self.hostname: str = platform.node()
        self.worker_id: str = f"{self.hostname}-{str(uuid.uuid4())}"

        self.logger.info("Registering exit handler")
        for signum, sigstr in {(signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")}:
            asyncio.get_event_loop().add_signal_handler(
                signum, lambda: asyncio.create_task(self.stop(f"{sigstr} received"))
            )

    async def __announce_worker(self, function_name: str) -> None:
        worker_data = {"hostname": self.worker_id, "joined_at": datetime.datetime.now()}

        current_fn_data: dict[str, Any] = {}

        if await self.redis_client.hexists(
            f"{self.key_prefix}/functions", function_name
        ):
            self.logger.info(f"Updating function {function_name}")
            current_fn_data: dict[str, dict] = unified_json_loads(
                await self.redis_client.hget(
                    f"{self.key_prefix}/functions", function_name
                )
            )

        if "workers" not in current_fn_data:
            current_fn_data["workers"] = {}

        current_fn_data["workers"][self.worker_id] = worker_data

        await self.redis_client.hset(
            f"{self.key_prefix}/functions",
            function_name,
            unified_json_dumps(current_fn_data),
        )
        self.logger.info(
            f"Announced worker {self.worker_id} for function {function_name}"
        )

    async def __deannounce_worker(self, function_name: str) -> None:
        self.logger.info(
            f"Deannouncing worker {self.worker_id} for function {function_name}"
        )
        current_fn_data: dict[str, Any] = unified_json_loads(
            await self.redis_client.hget(f"{self.key_prefix}/functions", function_name)
        )

        del current_fn_data["workers"][self.worker_id]

        if len(current_fn_data["workers"]) == 0:
            await self.redis_client.hdel(f"{self.key_prefix}/functions", function_name)
        else:
            await self.redis_client.hset(
                f"{self.key_prefix}/functions",
                function_name,
                unified_json_dumps(current_fn_data),
            )
        self.logger.info(
            f"Deannounced worker {self.worker_id} for function {function_name}"
        )

    def add_worker(self, function_name: str, worker: AsyncWorker) -> None:
        self.workers[function_name] = worker

    async def __await_for_input(
        self, function_name: str, input_num: int = 0
    ) -> dict[str, Any]:
        _t0 = time.monotonic()
        fn_input: bytes = (
            await self.redis_client.blpop([f"{self.key_prefix}/{function_name}/input"])
        )[1]
        _t1 = time.monotonic()
        self.logger.info(
            f"Received input #{input_num} for function {function_name} after {_t1 - _t0} seconds | Input: {fn_input}"
        )
        fn_input: dict[str, Any] = unified_json_loads(fn_input)
        req_id: str = fn_input["req_id"]
        fn_input: dict[str, Any] = fn_input["input"]
        return req_id, fn_input

    async def __process_input(
        self, function_name: str, worker: AsyncWorker, fn_input: dict[str, Any]
    ) -> tuple[str | None, dict[str, Any] | None, float]:
        _t0 = time.monotonic()
        error: str | None = None
        try:
            fn_output: any = await worker(fn_input)
        except Exception as e:
            self.logger.error(f"Error processing input for function {function_name}")
            self.logger.error(e)
            error = str(e)
            fn_output = None

        _t1 = time.monotonic()

        return error, fn_output, _t1 - _t0

    async def __send_output(
        self,
        function_name: str,
        req_id: str,
        error: str | None,
        fn_output: dict[str, Any] | None,
        processing_time: float,
    ) -> None:
        fn_output: dict[str, any] = {
            "ready_at": datetime.datetime.now(),
            "worker": self.worker_id,
            "processing_time": processing_time,
            "error": error,
            "output": fn_output,
        }
        fn_output: bytes = unified_json_dumps(fn_output)
        self.logger.info(
            f"Output message for function {function_name} has size {len(fn_output)} bytes"
        )
        await self.redis_client.lpush(
            f"{self.key_prefix}/{function_name}/output/{req_id}", fn_output
        )

    async def make_loop_fn(self, function_name: str, worker: AsyncWorker) -> None:
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
                    req_id, fn_input = await self.__await_for_input(
                        function_name, input_counter
                    )

                    error, fn_output, processing_time = await self.__process_input(
                        function_name, worker, fn_input
                    )
                    logger.info(
                        f"Processed input #{input_counter} for function {function_name} in {processing_time} seconds | Error: {error} | Output: {fn_output}"
                    )
                    await self.__send_output(
                        function_name, req_id, error, fn_output, processing_time
                    )
                    input_counter += 1

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
