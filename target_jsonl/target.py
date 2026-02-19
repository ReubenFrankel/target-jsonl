"""JSONL target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_jsonl.sinks import (
    JSONLSink,
)


class TargetJSONL(Target):
    """Target for JSONL."""

    name = "target-jsonl"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "output_dir",
            th.StringType(nullable=False),
            title="Output Directory",
            description="Path to the target output directory",
            default="output",
        ),
        th.Property(
            "overwrite",
            th.BooleanType(nullable=False),
            title="Overwrite",
            description="Overwrite output files atomically",
            default=False,
        ),
    ).to_dict()

    default_sink_class = JSONLSink


if __name__ == "__main__":
    TargetJSONL.cli()
