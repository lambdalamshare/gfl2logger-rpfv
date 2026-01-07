import asyncio
import csv
import logging
from collections.abc import Generator
from typing import Any

from google.protobuf import json_format
from mitmproxy import ctx, log
from embed import REMOLDINGS

import base64
import json

from generated.remoldings_pb2 import Remoldings
from gfl2logger.gfl2.data.base import BaseData

logger = logging.getLogger(__name__)


class RemoldingsData(BaseData):
    OPTIONS = [
        {
            "name": "gfl2_remoldings",
            "label": "Remoldings",
            "typespec": bool,
            "default": True,
            "help": "Log Remoldings on login",
        }
    ]

    async def export(self) -> None:
        if ctx.options.gfl2_remoldings:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.to_files)

    def to_raw_dicts(self) -> Generator[dict[str, Any]]:
        for b in self.data:
            remoldings = Remoldings()
            remoldings.ParseFromString(b)
            yield json_format.MessageToDict(remoldings)

    def to_dicts(self) -> Generator[dict[str, Any]]:
        for data in self.to_raw_dicts():
            for row in data["remoldings"]:
                b64 = row.get("contents")
                raw = base64.b64decode(b64)
                
                hex_str = " ".join(f"{b:02x}" for b in raw)
                hex_codes = [h for h in hex_str.split() if h != "01"]

                code1 = " ".join(hex_codes[0:3])
                code2 = " ".join(hex_codes[3:6])
                code3 = " ".join(hex_codes[6:9])
                
                value1 = REMOLDINGS.get(code1, None)
                value2 = REMOLDINGS.get(code2, None)
                value3 = REMOLDINGS.get(code3, None)
                
                yield {
                    "uid": "U"+str(row.get("uid")),
                    "stat1": value1,
                    "stat2": value2,
                    "stat3": value3,
                }

    def to_csv(self) -> None:
        filename = f"gfl2logger_remoldings_{self.log_time.strftime('%Y%m%dT%H%M%SZ')}.csv"
        cols = [
            "uid",
            "stat1",
            "stat2",
            "stat3",
        ]
        data = self.to_dicts()

        try:
            with open(filename, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(data)
            logger.log(log.ALERT, f"Remoldings csv data written to {filename}")
        except OSError as e:
            logger.error(f"Failed to write to {filename}, error={e}")
    
    def to_json(self) -> None:
        filename = (
            f"gfl2logger_remoldings_{self.log_time.strftime('%Y%m%dT%H%M%SZ')}.json"
        )
        data = list(self.to_dicts())
        
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.log(log.ALERT, f"Remoldings json data written to {filename}")
        except OSError as e:
            logger.error(f"Failed to write to {filename}, error={e}")
            
    def to_files(self) -> None:
        self.to_csv()
        self.to_json()
    
    