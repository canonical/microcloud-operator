"""Snap detection helpers for the microcloud charm.

The charm never installs or manages MicroCloud component snaps itself
(MicroCloud is deployed out-of-band); this module only detects which
snaps are present so metrics collection can be gated per-component.
"""

from charmlibs import snap as _snap


def is_installed(name: str) -> bool:
    """Return True if the named snap is installed."""
    try:
        _snap.list_one(name)
    except _snap.NotInstalledError:
        return False
    except _snap.Error:
        # snapd unreachable, or some other unexpected error: treat as
        # "not confirmed installed" rather than raising into the charm.
        return False
    return True
