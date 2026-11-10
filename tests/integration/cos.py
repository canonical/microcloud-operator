"""COS Lite deployment, relations and end-to-end observability queries."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from typing import Any

import jubilant
from tests.integration.helpers import (
    COS_TIMEOUT_IN_SECONDS,
    retry,
)

logger = logging.getLogger(__name__)

COS_CHANNEL = os.environ.get("COS_CHANNEL", "stable")
OTEL_CHANNEL = os.environ.get("OTEL_CHANNEL", "0.130/stable")
OTEL_BASE = os.environ.get("OTEL_BASE", "ubuntu@26.04")
COS_OFFER_USER = os.environ.get("COS_OFFER_USER", "admin")

COS_BACKENDS = ("prometheus", "loki", "grafana", "alertmanager", "catalogue")
COS_OFFERS = (
    "prometheus:receive-remote-write",
    "loki:logging",
    "grafana:grafana-dashboard",
)


def deploy_cos(juju: jubilant.Juju) -> None:
    """Deploy COS Lite and wait for the observability backends to become active."""
    logger.info("Deploying COS Lite (%s)", COS_CHANNEL)
    juju.deploy("cos-lite", channel=COS_CHANNEL, trust=True)
    wait_cos(juju)
    wait_traefik_ingress(juju)


def _proxied_endpoints(juju: jubilant.Juju) -> dict[str, str]:
    """Return traefik's proxied endpoints as a ``{app: url}`` mapping.

    The URLs point at traefik's external LoadBalancer address (from MetalLB).
    Returns an empty mapping until traefik has its external address.
    """
    task = juju.run("traefik/0", "show-proxied-endpoints")
    raw = task.results.get("proxied-endpoints", "{}")
    parsed = json.loads(raw)
    # Ingress-per-unit apps (prometheus, loki) are keyed by unit name
    # (e.g. "prometheus/0"); collapse to the application name.
    return {app.split("/")[0]: data["url"] for app, data in parsed.items() if "url" in data}


def wait_traefik_ingress(juju: jubilant.Juju) -> dict[str, str]:
    """Wait until traefik proxies the COS backends the tests query.

    Metrics and logs are pushed and queried through traefik's external address,
    so it must be ready before the charm is related to COS.
    """

    def ready() -> dict[str, str]:
        endpoints = _proxied_endpoints(juju)
        missing = {"prometheus", "loki"} - endpoints.keys()
        assert not missing, f"traefik is not proxying {missing} yet: {endpoints}"
        return endpoints

    return retry(
        ready,
        attempts=60,
        delay=10,
        description="traefik external ingress for prometheus and loki",
    )


def wait_cos(juju: jubilant.Juju, timeout: int = COS_TIMEOUT_IN_SECONDS) -> None:
    """Wait for the COS applications the tests consume to be active."""

    def ready(status: jubilant.Status) -> bool:
        for app_name in COS_BACKENDS:
            app = status.apps.get(app_name)
            if app is None or not app.units:
                return False
            if not all(unit.workload_status.current == "active" for unit in app.units.values()):
                return False
        return True

    # No `error=` callback: COS Lite's third-party charms hit transient hook
    # errors during pod restarts. With `automatically-retry-hooks` enabled, Juju
    # retries them, so wait for `ready` and let the timeout catch a genuine hang.
    juju.wait(
        ready,
        timeout=timeout,
        delay=10,
    )


def offer_cos(juju: jubilant.Juju) -> None:
    """Create the COS offers in the COS model."""
    for offer in COS_OFFERS:
        app, endpoint = offer.split(":", 1)
        logger.info("Offering %s", offer)
        juju.offer(app, endpoint=endpoint)


def _cos_model_parts(juju: jubilant.Juju) -> tuple[str, str]:
    model = juju.model or ""
    if ":" in model:
        controller, name = model.split(":", 1)
        return controller, name
    return "", model


def consume_cos(juju: jubilant.Juju, cos_juju: jubilant.Juju) -> None:
    """Consume the COS offers into the MicroCloud model."""
    controller, model = _cos_model_parts(cos_juju)
    prefix = (
        f"{controller}:{COS_OFFER_USER}/{model}" if controller else f"{COS_OFFER_USER}/{model}"
    )
    for app in ("prometheus", "loki", "grafana"):
        logger.info("Consuming %s.%s", prefix, app)
        juju.consume(f"{prefix}.{app}")


def relate_cos(juju: jubilant.Juju, cos_juju: jubilant.Juju) -> None:
    """Consume COS, deploy the collector and relate it to the charm."""
    consume_cos(juju, cos_juju)

    logger.info("Deploying opentelemetry-collector (%s, %s)", OTEL_CHANNEL, OTEL_BASE)
    juju.deploy("opentelemetry-collector", channel=OTEL_CHANNEL, base=OTEL_BASE)

    # Retain LXD's Loki stream labels when the collector forwards logs over the
    # OTLP endpoint; without this processor the labels are dropped and the logs
    # cannot be selected by `{app="lxd"}` in Loki.
    # https://canonical.com/microcloud/docs/latest/how-to/charm_metrics/#retain-loki-log-labels
    juju.config(
        "opentelemetry-collector",
        {
            "processors": (
                "attributes/lxd-loki-labels:\n"
                "  actions:\n"
                "    - action: upsert\n"
                "      key: loki.attribute.labels\n"
                '      value: "instance, app, location, name, project, type"\n'
            )
        },
    )

    for left, right in (
        ("opentelemetry-collector", "microcloud:cos-agent"),
        ("opentelemetry-collector", "microcloud:logging"),
        ("opentelemetry-collector", "prometheus"),
        ("opentelemetry-collector", "loki"),
        ("opentelemetry-collector", "grafana"),
    ):
        juju.integrate(left, right)

    juju.wait(
        lambda status: jubilant.all_active(status, "opentelemetry-collector"),
        error=lambda status: jubilant.any_error(status, "opentelemetry-collector"),
        timeout=COS_TIMEOUT_IN_SECONDS,
        delay=10,
    )


def _curl(url: str, *args: str) -> str:
    """Run ``curl`` from the runner and return its stdout."""
    result = subprocess.run(
        ["curl", "-fsS", *args, url],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl {url} failed ({result.returncode}): {result.stderr.strip()}")
    return result.stdout


def _json_from_output(output: str) -> dict[str, Any]:
    start = output.find("{")
    end = output.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"no JSON object in output: {output!r}")
    return json.loads(output[start : end + 1])


def _prometheus_query(juju: jubilant.Juju, query: str) -> dict[str, Any]:
    url = _proxied_endpoints(juju)["prometheus"]
    return _json_from_output(
        _curl(
            f"{url}/api/v1/query",
            "-G",
            "--data-urlencode",
            f"query={query}",
        )
    )


def assert_prometheus_targets(juju: jubilant.Juju, services: set[str]) -> None:
    """Assert that Prometheus is scraping every requested MicroCloud service."""

    def check_service(service: str) -> None:
        for query in (
            f'up{{microcloud_service="{service}"}}',
            f'up{{job=~".*microcloud-{service}.*"}}',
        ):
            response = _prometheus_query(juju, query)
            results = response.get("data", {}).get("result", [])
            healthy = [result for result in results if result.get("value", [None, ""])[1] == "1"]
            if healthy:
                return
        raise AssertionError(f"no healthy Prometheus target for microcloud_service={service}")

    for service in sorted(services):
        retry(
            lambda service=service: check_service(service),
            attempts=60,
            delay=10,
            description=f"healthy Prometheus target for {service}",
        )


def _loki_query(juju: jubilant.Juju, query: str, minutes: int = 15) -> dict[str, Any]:
    now = int(time.time())
    url = _proxied_endpoints(juju)["loki"]
    return _json_from_output(
        _curl(
            f"{url}/loki/api/v1/query_range",
            "-G",
            "--data-urlencode",
            f"query={query}",
            "--data-urlencode",
            f"start={(now - minutes * 60) * 1_000_000_000}",
            "--data-urlencode",
            f"end={now * 1_000_000_000}",
            "--data-urlencode",
            "limit=10",
            "--data-urlencode",
            "direction=backward",
        )
    )


def assert_loki_has_lxd_logs(juju: jubilant.Juju) -> None:
    """Assert that LXD logs made it all the way to Loki."""

    def check() -> None:
        response = _loki_query(juju, '{app="lxd"}')
        streams = response.get("data", {}).get("result", [])
        if any(stream.get("values") for stream in streams):
            return
        raise AssertionError("no LXD log streams found in Loki")

    retry(
        check,
        attempts=60,
        delay=10,
        description="LXD logs in Loki",
    )
