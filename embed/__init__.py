import lzma
import pickle
import pkgutil
import json
from typing import Any


def _load_embed(resource: str) -> Any:
    data = pkgutil.get_data("embed", resource)
    if data is not None:
        return pickle.loads(lzma.decompress(data))
    return None

def _load_remoldings_table(resource:str) -> Any:
    data = pkgutil.get_data("embed", resource)
    if data is not None:
        json_string = data.decode('utf-8')
        raw_data = json.loads(json_string)
        return {item["code"]: item["name"] for item in raw_data}
    return None


ICON_ICO = pkgutil.get_data("embed", "icon.ico")
ICON_PNG = pkgutil.get_data("embed", "icon.png")

_GFL2: dict[str, Any] = _load_embed("gfl2.xz")

ATTACHMENT_EFFECTS: dict[int, str] = _GFL2["ATTACHMENT_EFFECTS"]
ATTACHMENTS: dict[int, dict[str, str]] = _GFL2["ATTACHMENTS"]
ATTRIBUTES_IS_PERCENT: dict[int, bool] = _GFL2["ATTRIBUTES_IS_PERCENT"]
ATTRIBUTES_NAME_STRIPPED: dict[int, str] = _GFL2["ATTRIBUTES_NAME_STRIPPED"]
DOLLS: dict[int, str] = _GFL2["DOLLS"]
KEYS: dict[int, str] = _GFL2["KEYS"]
WEAPONS: dict[int, str] = _GFL2["WEAPONS"]

REMOLDINGS: dict[str, str] = _load_remoldings_table("remoldings_table.txt")