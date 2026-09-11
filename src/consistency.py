"""Per-unit MicroCloud membership consistency check for the microcloud charm.

The charm does not deploy or bootstrap MicroCloud.
It requires an already bootstrapped MicroCloud to place its units on top.
"""


def validate_membership(hostname: str, member_names: set[str]) -> str | None:
    """Validate that this unit's hostname is a MicroCloud member.

    Returns a human-readable problem string if this unit's hostname is not
    a MicroCloud member, or None if it is.
    """
    if hostname not in member_names:
        return f"Juju unit on {hostname!r} is not a MicroCloud member"
    return None
