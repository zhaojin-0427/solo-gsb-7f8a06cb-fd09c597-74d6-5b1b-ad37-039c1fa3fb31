"""Single-image entrypoint. Role is selected via ``SERVICE_ROLE``.

* ``coordinator`` (default) - serves the object HTTP API and keeps SQLite
  metadata (never object bytes) in ``/data/coordinator.db``.
* ``node`` - serves a shard node, storing shards in ``/data/node.db``.

``app_factory`` is the uvicorn import target so configuration is read from
the process environment at startup.
"""

from __future__ import annotations

import os

import uvicorn


def app_factory():
    role = os.environ.get("SERVICE_ROLE", "coordinator").lower()
    if role == "node":
        from .node import create_node_app

        db_path = os.environ.get("DB_PATH", "/data/node.db")
        node_name = os.environ.get("NODE_NAME", "node")
        return create_node_app(db_path, node_name=node_name)

    from .coordinator import create_app

    db_path = os.environ.get(
        "DB_PATH", os.environ.get("COORDINATOR_DB", "/data/coordinator.db")
    )
    return create_app(db_path=db_path)


def main() -> None:
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(
        "app.main:app_factory",
        factory=True,
        host="0.0.0.0",
        port=port,
    )


if __name__ == "__main__":
    main()
