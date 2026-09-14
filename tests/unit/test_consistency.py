"""Unit tests for consistency.validate_membership."""

from consistency import validate_membership


def test_validate_membership_hostname_is_member():
    assert validate_membership("host-a", {"host-a", "host-b"}) is None


def test_validate_membership_hostname_not_member():
    problem = validate_membership("host-a", {"host-b", "host-c"})
    assert problem == "Juju unit on 'host-a' is not a MicroCloud member"


def test_validate_membership_empty_members():
    problem = validate_membership("host-a", set())
    assert problem == "Juju unit on 'host-a' is not a MicroCloud member"
