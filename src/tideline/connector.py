"""Kafka Connect REST client for registering the Debezium connector.

`connectors/postgres-source.json` carries inline `//`-prefixed keys explaining
each setting, because a connector config is mostly non-obvious knobs and a
config nobody understands is a config nobody can change. Kafka Connect would
reject those keys, so they are stripped here on the way out — the file stays
readable and the request stays valid.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_CONNECT_URL = "http://localhost:8083"


def load_connector_config(path: Path) -> dict:
    """Read the connector definition, dropping documentation keys."""
    raw = json.loads(path.read_text())
    config = {k: v for k, v in raw["config"].items() if not k.startswith("//")}
    return {"name": raw["name"], "config": config}


def _request(url: str, method: str = "GET", payload: dict | None = None, timeout: float = 30):
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(  # noqa: S310 - fixed localhost URL
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
        body = response.read().decode()
        return response.status, (json.loads(body) if body else None)


def wait_for_connect(base_url: str = DEFAULT_CONNECT_URL, timeout_seconds: float = 120) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            _request(f"{base_url}/connectors", timeout=5)
            return True
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(2)
    return False


def register(
    config_path: Path,
    base_url: str = DEFAULT_CONNECT_URL,
    *,
    replace: bool = True,
) -> dict:
    """Create or update the connector.

    Uses `PUT /connectors/<name>/config`, which is idempotent — unlike
    `POST /connectors`, which returns 409 if the connector already exists and
    turns a re-run of the setup script into a failure.
    """
    definition = load_connector_config(config_path)
    name = definition["name"]

    if replace:
        status, body = _request(
            f"{base_url}/connectors/{name}/config", method="PUT", payload=definition["config"]
        )
    else:
        status, body = _request(f"{base_url}/connectors", method="POST", payload=definition)

    log.info("registered connector %s (HTTP %s)", name, status)
    return body or {}


def status(name: str, base_url: str = DEFAULT_CONNECT_URL) -> dict:
    _, body = _request(f"{base_url}/connectors/{name}/status")
    return body or {}


def delete(name: str, base_url: str = DEFAULT_CONNECT_URL) -> None:
    try:
        _request(f"{base_url}/connectors/{name}", method="DELETE")
        log.info("deleted connector %s", name)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise


def list_connectors(base_url: str = DEFAULT_CONNECT_URL) -> list[str]:
    _, body = _request(f"{base_url}/connectors")
    return body or []


def wait_until_running(
    name: str,
    base_url: str = DEFAULT_CONNECT_URL,
    timeout_seconds: float = 120,
) -> dict:
    """Block until the connector and its task both report RUNNING.

    A connector can be RUNNING while its task has already FAILED — checking
    only the connector reports success on a pipeline that is not moving.
    """
    deadline = time.monotonic() + timeout_seconds
    last: dict = {}
    while time.monotonic() < deadline:
        try:
            last = status(name, base_url)
        except urllib.error.HTTPError:
            time.sleep(2)
            continue

        connector_state = last.get("connector", {}).get("state")
        tasks = last.get("tasks", [])
        task_states = [t.get("state") for t in tasks]

        if connector_state == "FAILED":
            raise RuntimeError(f"connector failed: {last.get('connector', {}).get('trace')}")
        if any(s == "FAILED" for s in task_states):
            failed = next(t for t in tasks if t.get("state") == "FAILED")
            raise RuntimeError(f"connector task failed: {failed.get('trace')}")
        if (
            connector_state == "RUNNING"
            and task_states
            and all(s == "RUNNING" for s in task_states)
        ):
            return last
        time.sleep(2)

    raise TimeoutError(f"connector {name} not RUNNING after {timeout_seconds}s: {last}")
