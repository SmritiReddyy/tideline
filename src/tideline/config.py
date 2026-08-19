"""Runtime configuration for Tideline.

Environment-driven so the same code runs from a laptop, a container, and a
cluster without edits.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    return default if value is not None and value.strip() == "" else value


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    return int(raw) if raw else default


@dataclass(frozen=True)
class Config:
    # --- source database ---
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str

    # --- streaming backbone ---
    bootstrap_servers: str
    # Debezium prefixes every topic with the logical server name.
    topic_prefix: str
    pg_schema: str

    # --- lakehouse ---
    lakehouse_root: str
    checkpoint_root: str

    # --- spark ---
    driver_memory: str
    shuffle_partitions: int
    max_offsets_per_trigger: int
    trigger_interval: str

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_database} "
            f"user={self.pg_user} password={self.pg_password}"
        )

    def topic_for(self, table: str) -> str:
        """Debezium topic naming: <server>.<schema>.<table>."""
        return f"{self.topic_prefix}.{self.pg_schema}.{table}"

    def table_path(self, table: str) -> str:
        return f"{self.lakehouse_root.rstrip('/')}/{table}"

    def checkpoint_path(self, table: str) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{table}"

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            pg_host=_env("TIDELINE_PG_HOST", "localhost"),
            pg_port=_env_int("TIDELINE_PG_PORT", 5432),
            pg_database=_env("TIDELINE_PG_DATABASE", "tideline"),
            pg_user=_env("TIDELINE_PG_USER", "tideline"),
            pg_password=_env("TIDELINE_PG_PASSWORD", "tideline"),
            bootstrap_servers=_env("TIDELINE_BOOTSTRAP_SERVERS", "localhost:19092"),
            topic_prefix=_env("TIDELINE_TOPIC_PREFIX", "tideline"),
            pg_schema=_env("TIDELINE_PG_SCHEMA", "shop"),
            lakehouse_root=_env("TIDELINE_LAKEHOUSE_ROOT", str(REPO_ROOT / "data" / "lakehouse")),
            checkpoint_root=_env(
                "TIDELINE_CHECKPOINT_ROOT", str(REPO_ROOT / "data" / "checkpoints")
            ),
            driver_memory=_env("TIDELINE_DRIVER_MEMORY", "2g"),
            shuffle_partitions=_env_int("TIDELINE_SHUFFLE_PARTITIONS", 8),
            # Bounding the batch size keeps the first (snapshot) batch from
            # trying to load the entire backfill into one micro-batch.
            max_offsets_per_trigger=_env_int("TIDELINE_MAX_OFFSETS_PER_TRIGGER", 20000),
            trigger_interval=_env("TIDELINE_TRIGGER_INTERVAL", "10 seconds"),
        )


def get_config() -> Config:
    return Config.from_env()
