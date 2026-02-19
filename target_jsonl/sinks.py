"""JSONL target sink class, which handles writing streams."""

from __future__ import annotations

import pathlib
from functools import cached_property

from singer_sdk.singerlib.json import serialize_json
from singer_sdk.sinks import BatchSink
from typing_extensions import override


class JSONLSink(BatchSink):
    """JSONL target sink class."""

    max_size = 10000  # Max records to write in one batch

    @cached_property
    def overwrite(self) -> bool:
        return self.config["overwrite"]

    @cached_property
    def output_dir(self):
        return pathlib.Path(self.config["output_dir"]).resolve(strict=True)

    @cached_property
    def filename(self):
        return f"{self.stream_name}.jsonl"

    @cached_property
    def filepath(self):
        return self.output_dir / self.filename

    @cached_property
    def temp_filepath(self):
        filename = f".{self.filename}.{self.sync_started_at}.tmp"
        return self.output_dir / filename

    @override
    def process_batch(self, context):
        filepath = self.temp_filepath if self.overwrite else self.filepath
        lines = [serialize_json(r) for r in context["records"]]

        try:
            with filepath.open("a") as f:
                content = "\n".join(lines) + "\n" if lines else ""
                f.write(content)
        except Exception:
            self.temp_filepath.unlink(missing_ok=True)
            raise

    @override
    def clean_up(self):
        super().clean_up()

        if self.overwrite and self.temp_filepath.exists():
            self.temp_filepath.replace(self.filepath)
