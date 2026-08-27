"""Host-side readiness probes for the local stack.

Compose healthchecks cover the services whose images contain a shell. The
OpenTelemetry Collector image is distroless -- no `/bin/sh`, no `wget` -- and a
Docker healthcheck can only execute *inside* the container, so there is no way
to express its readiness in the compose file. Probing from the host is the
honest alternative, and it doubles as the wait that integration tests need.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.request
from collections.abc import Callable

from telemetry_engine.common.logging import get_logger

log = get_logger(__name__)


def wait_for_http(
    url: str,
    *,
    predicate: Callable[[str], bool] | None = None,
    timeout_s: float = 60.0,
    interval_s: float = 1.0,
    name: str | None = None,
) -> None:
    """Poll `url` until it responds 2xx (and satisfies `predicate`), or raise.

    `predicate` inspects the response body, for endpoints that return 200 with a
    body saying they are not actually ready.
    """
    label = name or url
    deadline = time.monotonic() + timeout_s
    last_error: str = "no attempt made"

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310 - fixed local URL
                body = resp.read().decode("utf-8", errors="replace")
                if 200 <= resp.status < 300 and (predicate is None or predicate(body)):
                    return
                last_error = f"status={resp.status} body={body[:120]!r}"
        except (urllib.error.URLError, OSError) as exc:
            last_error = str(exc)
        time.sleep(interval_s)

    raise TimeoutError(f"{label} not ready after {timeout_s}s: {last_error}")
