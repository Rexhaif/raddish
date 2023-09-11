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
import uuid
from typing import Any

import orjson as json
from redis import Redis as SyncRedis
from redis.asyncio import Redis as AsyncRedis


class RaddishRemoteExecutionException(Exception):
    def __init__(self, function_name: str, msg: str | None = None) -> None:
        super().__init__(
            f"Exception raised during remote execution of function {function_name}: {msg}"
        )


class AsyncRaddishClient:
    def __init__(self, redis_client: AsyncRedis, key_prefix: str = "raddish") -> None:
        self.redis_client: AsyncRedis = redis_client
        self.key_prefix: str = key_prefix
        self.logger: logging.Logger = logging.getLogger("raddish-client")

    async def __submit_job(self, function_name: str, input: dict[str, Any]) -> str:
        req_id: str = str(uuid.uuid4())
        fn_input: dict[str, Any] = {
            "req_id": req_id,
            "input": input,
        }
        fn_input: bytes = json.dumps(fn_input)
        self.logger.info(f"Sending input for function {function_name}")
        await self.redis_client.lpush(
            f"{self.key_prefix}/{function_name}/input", fn_input
        )
        return req_id

    async def __get_output(
        self, function_name: str, req_id: str, timeout: int = 360
    ) -> dict[str, Any]:
        self.logger.info(
            f"Waiting for output for function {function_name}, timeout: {timeout} seconds, request ID: {req_id}"
        )
        fn_output: list[bytes] | None = await self.redis_client.blpop(
            [f"{self.key_prefix}/{function_name}/output/{req_id}"], timeout=timeout
        )
        if fn_output is None:
            raise TimeoutError(
                f"Timeout waiting for output for function {function_name} request {req_id}"
            )

        fn_output: bytes = fn_output[1]

        self.logger.info(f"Received output for function {function_name}")
        fn_output: dict[str, any] = json.loads(fn_output)
        return fn_output

    async def call(
        self, function_name: str, input: dict[str, Any], timeout: int = 360
    ) -> dict[str, Any]:
        req_id: str = await self.__submit_job(function_name, input)
        fn_output: dict[str, Any] = await self.__get_output(
            function_name, req_id, timeout=timeout
        )
        if fn_output["error"] is not None:
            raise RaddishRemoteExecutionException(
                function_name=function_name, msg=fn_output["error"]
            )
        else:
            self.logger.info(
                f"Output for function {function_name} was ready at {fn_output['ready_at']}, processed by {fn_output['worker']} in {fn_output['processing_time']} seconds"
            )
            return fn_output["output"]

    async def batch_call(
        self,
        function_name: str | list[str],
        inputs: list[dict[str, Any]],
        timeout: int = 360,
        raise_on_error: bool = True,
    ) -> list[dict[str, Any]]:
        if isinstance(function_name, str):
            function_name: list[str] = [function_name] * len(inputs)
        elif len(function_name) != len(inputs):
            raise ValueError(
                "Number of functions and inputs must be equal, alternatively pass a single function name"
            )

        req_ids: list[str] = [
            await self.__submit_job(function_name[i], inputs[i])
            for i in range(len(inputs))
        ]

        fn_outputs: list[dict[str, Any]] = await asyncio.gather(
            *[
                self.__get_output(function_name[i], req_ids[i], timeout=timeout)
                for i in range(len(inputs))
            ]
        )

        outputs = []
        for i, output in enumerate(fn_outputs):
            if output["error"] is not None:
                if raise_on_error:
                    raise RaddishRemoteExecutionException(
                        function_name[i], output["error"]
                    )
                else:
                    outputs.append(None)
                    self.logger.warn(
                        f"Erorr processing input #{i} for function {function_name[i]}: {output['error']}"
                    )
            else:
                self.logger.info(
                    f"Output for function {function_name} was ready at {output['ready_at']}, processed by {output['worker']} in {output['processing_time']} seconds"
                )
                outputs.append(output["output"])

        return outputs


class SyncRaddishClient:
    def __init__(self, redis_client: SyncRedis, key_prefix: str = "raddish") -> None:
        self.redis_client: SyncRedis = redis_client
        self.key_prefix: str = key_prefix
        self.logger: logging.Logger = logging.getLogger("raddish-client")

    def __submit_job(self, function_name: str, input: dict[str, Any]) -> str:
        req_id: str = str(uuid.uuid4())
        fn_input: dict[str, Any] = {
            "req_id": req_id,
            "input": input,
        }
        fn_input: bytes = json.dumps(fn_input)
        self.logger.info(f"Sending input for function {function_name}")
        self.redis_client.lpush(f"{self.key_prefix}/{function_name}/input", fn_input)
        return req_id

    def __get_output(
        self, function_name: str, req_id: str, timeout: int = 360
    ) -> dict[str, Any]:
        self.logger.info(
            f"Waiting for output for function {function_name}, timeout: {timeout} seconds"
        )
        fn_output: list[bytes] | None = self.redis_client.blpop(
            [f"{self.key_prefix}/{function_name}/output/{req_id}"], timeout=timeout
        )
        if fn_output is None:
            raise TimeoutError(
                f"Timeout waiting for output for function {function_name} request {req_id}"
            )

        fn_output: bytes = fn_output[1]
        self.logger.info(f"Received output for function {function_name}")
        fn_output: dict[str, any] = json.loads(fn_output)
        return fn_output

    def call(
        self, function_name: str, input: dict[str, Any], timeout: int = 360
    ) -> dict[str, Any]:
        req_id: str = self.__submit_job(function_name, input)
        fn_output: dict[str, Any] = self.__get_output(
            function_name, req_id, timeout=timeout
        )
        if fn_output["error"] is not None:
            raise RaddishRemoteExecutionException(
                function_name=function_name, msg=fn_output["error"]
            )
        else:
            self.logger.info(
                f"Output for function {function_name} was ready at {fn_output['ready_at']}, processed by {fn_output['worker']} in {fn_output['processing_time']} seconds"
            )
            return fn_output["output"]

    def batch_call(
        self,
        function_name: str | list[str],
        inputs: list[dict[str, Any]],
        timeout: int = 360,
        raise_on_error: bool = True,
    ) -> list[dict[str, Any]]:
        if isinstance(function_name, str):
            function_name: list[str] = [function_name] * len(inputs)
        elif len(function_name) != len(inputs):
            raise ValueError(
                "Number of functions and inputs must be equal, alternatively pass a single function name"
            )

        req_ids: list[str] = [
            self.__submit_job(function_name[i], inputs[i]) for i in range(len(inputs))
        ]

        fn_outputs: list[dict[str, Any]] = [
            self.__get_output(function_name[i], req_ids[i], timeout=timeout)
            for i in range(len(inputs))
        ]

        outputs = []
        for i, output in enumerate(fn_outputs):
            if output["error"] is not None:
                if raise_on_error:
                    raise RaddishRemoteExecutionException(
                        function_name[i], output["error"]
                    )
                else:
                    outputs.append(None)
                    self.logger.warn(
                        f"Erorr processing input #{i} for function {function_name[i]}: {output['error']}"
                    )
            else:
                self.logger.info(
                    f"Output for function {function_name} was ready at {output['ready_at']}, processed by {output['worker']} in {output['processing_time']} seconds"
                )
                outputs.append(output["output"])

        return outputs
