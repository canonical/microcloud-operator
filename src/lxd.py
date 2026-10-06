"""Manages LXD configuration for observability in the microcloud charm.

Responsibilities
----------------
- Set/reset the LXD metrics listener (core.metrics_address /
  core.metrics_authentication) used for Prometheus scraping.
- Point LXD's native Loki client (loki.api.url) at a related Loki-compatible endpoint.
"""

import logging
from urllib.parse import urlsplit, urlunsplit

import pylxd

from constants import LOCALHOST_ADDR, LXD_METRICS_PORT

logger = logging.getLogger(__name__)

# Reduce verbosity of API calls made by pylxd
logging.getLogger("urllib3").setLevel(logging.WARNING)


class LXDConfigError(Exception):
    """Raised when an LXD configuration operation fails."""


class LXDManager:
    """Manages LXD's observability-related configuration."""

    def __init__(self, port: int = LXD_METRICS_PORT) -> None:
        self._port = port

    @property
    def metrics_address(self) -> str:
        """Address LXD's metrics listener is configured to bind to."""
        return f"{LOCALHOST_ADDR}:{self._port}"

    def ensure_metrics_config(self) -> None:
        """Set core.metrics_address and core.metrics_authentication on LXD.

        Raises
        ------
        LXDConfigError
            If the LXD config update fails.
        """
        self._lxc_config_set(
            {
                "core.metrics_address": self.metrics_address,
                "core.metrics_authentication": "false",
            }
        )

    def teardown_metrics_config(self) -> None:
        """Unset LXD's metrics listener configuration."""
        try:
            self._lxc_config_unset(["core.metrics_address", "core.metrics_authentication"])
        except LXDConfigError as exc:
            logger.warning("Cannot reset LXD metrics config during teardown: %s", exc)

    def ensure_loki_config(self, loki_endpoints: list) -> None:
        """Point LXD's native Loki client at the related Loki push endpoint."""
        if not loki_endpoints:
            logger.debug("logging relation joined but no Loki endpoint published yet")
            return

        url = loki_endpoints[0].get("url", "")
        if not url:
            logger.warning("Loki endpoint data is missing a url")
            return

        # LXD expects only the base API URL (protocol + host + optional port),
        # not the full push path.
        if url.endswith("/loki/api/v1/push"):
            url = url[: -len("/loki/api/v1/push")]

        # Ensure we always point to localhost and not to another OTLP
        # endpoint from a unit on another member which we might get through the relation.
        parsed = urlsplit(url)
        netloc = LOCALHOST_ADDR if not parsed.port else f"{LOCALHOST_ADDR}:{parsed.port}"
        url = urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))

        try:
            self._lxc_config_set(
                {
                    "loki.api.url": url,
                    # Disable the readiness check for the Loki API as the opentelemetry-collector OTLP endpoint doesn't support it.
                    "loki.api.check_ready": "false",
                }
            )
        except LXDConfigError as exc:
            logger.warning("Cannot set LXD loki.api.url: %s", exc)
            return

        logger.info("LXD is now streaming logs to Loki at %s", url)

    def teardown_loki_config(self) -> None:
        """Stop LXD from streaming logs to Loki."""
        try:
            self._lxc_config_unset(["loki.api.url", "loki.api.check_ready"])
        except LXDConfigError as exc:
            logger.warning("Cannot reset LXD loki.api.url during teardown: %s", exc)
            return

        logger.info("LXD is no longer streaming logs to Loki")

    def _lxc_config_set(self, config: dict) -> None:
        """Set one or more LXD server config keys via the LXD API.

        Equivalent to running `lxc config set <key> <value>` for each entry.
        Raise LXDConfigError on failure.
        """
        try:
            client = pylxd.Client()
            conf = client.api.get().json()["metadata"]["config"]
            conf.update(config)
            client.api.put(json={"config": conf})
        except (pylxd.exceptions.LXDAPIException, pylxd.exceptions.ClientConnectionFailed) as exc:
            raise LXDConfigError(f"Cannot set LXD config {config!r}: {exc}") from exc

    def _lxc_config_unset(self, keys: list) -> None:
        """Unset one or more LXD server config keys via the LXD API.

        Equivalent to running `lxc config unset <key>` for each entry.
        Raise LXDConfigError on failure.
        """
        try:
            client = pylxd.Client()
            conf = client.api.get().json()["metadata"]["config"]
            changed = False
            for key in keys:
                if conf.pop(key, None) is not None:
                    changed = True
            if changed:
                client.api.put(json={"config": conf})
        except (pylxd.exceptions.LXDAPIException, pylxd.exceptions.ClientConnectionFailed) as exc:
            raise LXDConfigError(f"Cannot unset LXD config {keys!r}: {exc}") from exc
