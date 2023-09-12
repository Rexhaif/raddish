import orjson as json
from typing import Any

def unified_json_dumps(obj: Any) -> str:
    return json.dumps(obj, option=json.OPT_SERIALIZE_NUMPY)

def unified_json_loads(obj: str) -> Any:
    return json.loads(obj)