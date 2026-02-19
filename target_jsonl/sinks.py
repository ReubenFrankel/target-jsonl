"""JSONL target sink class, which handles writing streams."""

from __future__ import annotations

import pathlib

from singer_sdk.singerlib.json import serialize_json
from singer_sdk.sinks import BatchSink
from typing_extensions import override


class JSONLSink(BatchSink):
    """JSONL target sink class."""

    max_size = 10000  # Max records to write in one batch

    @override
    def process_batch(self, context):
        output_dir = pathlib.Path(self.config["output_dir"]).resolve(strict=True)
        filepath = output_dir / f"{self.stream_name}.jsonl"

        with filepath.open("a") as f:
            f.writelines(serialize_json(r) + "\n" for r in context["records"])
