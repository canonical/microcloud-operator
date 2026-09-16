"""Pytest configuration for the MicroCloud charm integration tests.

The ``juju`` fixture (a module-scoped temporary model) comes from
``pytest-jubilant``; everything here is about pointing the tests at a locally
built charm and at the machine shape it needs.
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Iterator
from pathlib import Path

import jubilant
import pytest
import yaml

logger = logging.getLogger(__name__)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--charm",
        default="",
        action="store",
        help="Path to a charm built with `charmcraft pack`.",
    )
    parser.addoption(
        "--constraints",
        default="",
        action="store",
        help=(
            'Space-separated Juju model constraints, e.g. "mem=2G". Left '
            "empty the units are plain LXD containers, which is enough for an "
            "LXD-only MicroCloud; enabling MicroCeph or MicroOVN would need "
            "virt-type=virtual-machine and real disks."
        ),
    )
    parser.addoption(
        "--num-units",
        default=2,
        type=int,
        action="store",
        help=(
            "How many MicroCloud units to deploy. Two is the smallest that "
            "exercises the initiator/joiner handshake; one skips it entirely."
        ),
    )


@pytest.fixture(scope="module")
def charm_path(request: pytest.FixtureRequest) -> Path:
    path = request.config.getoption("--charm")
    assert path, "--charm must be provided; build the charm with `charmcraft pack` first"
    assert Path(path).is_file(), f"--charm points at a missing file: {path}"
    # Return a Path: jubilant only treats pathlib.Path as a local charm file,
    # so a bare filename would be looked up on Charmhub instead.
    return Path(path).absolute()


@pytest.fixture(scope="module")
def constraints(request: pytest.FixtureRequest) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for constraint in request.config.getoption("--constraints").split(" "):
        if not constraint:
            continue
        key, value = constraint.split("=", 1)
        parsed[key] = value
    return parsed


@pytest.fixture(scope="module")
def num_units(request: pytest.FixtureRequest) -> int:
    count = request.config.getoption("--num-units")
    assert count >= 1, "--num-units must be at least 1"
    return count


@pytest.fixture(scope="module")
def charm_name() -> str:
    metadata = yaml.safe_load(Path("charmcraft.yaml").read_text())
    return metadata["name"]


@pytest.fixture(scope="module", autouse=True)
def report_resources(request: pytest.FixtureRequest, juju: jubilant.Juju) -> Iterator[None]:
    """Log free disk and the model's machines once each module is done.

    A runner that runs out of disk or memory dies without uploading any logs,
    so record where each module left it while the live log still streams.
    """
    yield
    usage = shutil.disk_usage("/")
    logger.info(
        "After %s: %.1f GiB of %.1f GiB disk free",
        request.module.__name__,
        usage.free / 2**30,
        usage.total / 2**30,
    )
    try:
        logger.info("Model status:\n%s", juju.cli("status"))
    except Exception as exc:
        logger.warning("Cannot read the model status: %s", exc)
