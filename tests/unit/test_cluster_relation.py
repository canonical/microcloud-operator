# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the "microcloud_cluster" relation and member join sessions."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import ops
import ops.testing
import pytest

import session
import snap
from cluster import (
    ClusterCoordinator,
    JoinSession,
    PeerSystem,
    identity_fields,
    read_passphrase,
    system_from_data,
    validate_membership,
)
from cluster_relation import (
    ClusterRelation,
    JoinFailures,
    MemberSystem,
    OrchestratorFacts,
    _set,
    candidate_signatures,
)
from network import UnitNetwork

_META = """
name: microcloud
peers:
  cluster:
    interface: microcloud-peer
provides:
  members:
    interface: microcloud_cluster
requires:
  orchestrator:
    interface: microcloud_cluster
    limit: 1
"""


class _Databag(dict):
    """Counts writes and, like Juju, drops a key that is set to ""."""

    writes = 0

    def __setitem__(self, key, value):
        self.writes += 1
        if value == "":
            super().pop(key, None)
        else:
            super().__setitem__(key, value)


def _harness(app="microcloud"):
    harness = ops.testing.Harness(ops.CharmBase, meta=_META.replace("microcloud\n", f"{app}\n", 1))
    harness.set_leader(True)
    harness.begin()
    return harness


def _member_data(name, address, *, ready=True, versions=None, initialized=False):
    data = identity_fields(name, address, initialized=initialized)
    if ready:
        data["microcloud-ready"] = "true"
    data["service-versions"] = json.dumps(versions or {"lxd": "6.9"})
    return data


class TestClusterRelation:
    def test_member_reads_the_facts_the_orchestrator_published(self):
        orchestrator = _harness()
        rel_id = orchestrator.add_relation("members", "arm")
        relations = ClusterRelation(orchestrator.charm)
        relation = relations.members_relation(rel_id)
        opened = JoinSession("abc", "10.0.0.1", ["arm0"], 2000.0, relation_id=rel_id)
        relations.publish_role(True)
        relations.publish_facts(
            relation,
            snap_channels={"lxd": "6/stable", "microceph": "tentacle/stable"},
            service_versions={"lxd": "6.9"},
            initialized=True,
            fingerprint="abcd",
            storage=["local", "ceph"],
            secret_id="secret:123",
            session_timeout=600,
        )
        relations.publish_session(relation, opened)
        relations.record_failures(relation, JoinFailures(1, "boom", {"arm0": "sig"}))
        published = dict(relation.data[orchestrator.charm.app])

        member = _harness("arm")
        member_rel = member.add_relation("orchestrator", "microcloud", app_data=published)
        facts = ClusterRelation(member.charm).facts()

        assert ClusterRelation.remote_role(member.model.get_relation("orchestrator", member_rel))
        assert facts == OrchestratorFacts(
            snap_channels={"lxd": "6/stable", "microceph": "tentacle/stable"},
            service_versions={"lxd": "6.9"},
            initialized=True,
            fingerprint="abcd",
            storage=["ceph", "local"],
            secret_id="secret:123",
            session=opened,
            session_timeout=600,
            failures=JoinFailures(1, "boom", {"arm0": "sig"}),
        )

    def test_facts_are_none_without_the_relation_and_empty_before_publishing(self):
        member = _harness("arm")
        relations = ClusterRelation(member.charm)
        assert relations.facts() is None

        member.add_relation("orchestrator", "microcloud")
        facts = relations.facts()
        assert facts is not None
        assert facts.snap_channels is None
        assert facts.storage is None

    def test_values_this_hook_could_not_read_stay_as_published(self):
        orchestrator = _harness()
        rel_id = orchestrator.add_relation("members", "arm")
        relations = ClusterRelation(orchestrator.charm)
        relation = relations.members_relation(rel_id)
        facts = {
            "snap_channels": {"lxd": "6/stable"},
            "initialized": True,
            "session_timeout": 300,
        }
        relations.publish_facts(
            relation,
            **facts,
            service_versions={"lxd": "6.9"},
            fingerprint="abcd",
            storage=["local"],
            secret_id="secret:1",
        )
        relations.publish_facts(
            relation,
            **facts,
            service_versions=None,
            fingerprint=None,
            storage=None,
            secret_id=None,
        )

        relations.publish_facts(
            relation,
            snap_channels=None,
            service_versions=None,
            initialized=True,
            fingerprint=None,
            storage=None,
            secret_id=None,
            session_timeout=300,
        )

        data = relation.data[orchestrator.charm.app]
        assert data["snap-channels"] == '{"lxd": "6/stable"}'
        assert data["service-versions"] == '{"lxd": "6.9"}'
        assert data["cluster-fingerprint"] == "abcd"
        assert data["cluster-storage"] == '["local"]'
        assert data["session-passphrase-secret-id"] == "secret:1"

    def test_set_writes_only_on_change(self):
        """Also for "", which Juju drops, so it never reads back as stored."""
        data = _Databag()

        for _ in range(2):
            _set(data, "name", "arm0")
            _set(data, "uplink", "")
            _set(data, "fingerprint", None)

        assert data == {"name": "arm0"}
        assert data.writes == 1

        _set(data, "name", "")
        assert data == {}

    def test_member_systems_and_units(self):
        orchestrator = _harness()
        rel_id = orchestrator.add_relation("members", "arm")
        orchestrator.add_relation_unit(rel_id, "arm/0")
        orchestrator.add_relation_unit(rel_id, "arm/1")
        orchestrator.update_relation_data(
            rel_id, "arm/0", _member_data("arm0", "10.0.1.1", versions={"lxd": "6.9"})
        )
        orchestrator.update_relation_data(rel_id, "arm/1", {"microcloud-name": "arm1"})
        relations = ClusterRelation(orchestrator.charm)
        relation = relations.members_relation(rel_id)

        assert relations.member_units(relation) is None
        orchestrator.update_relation_data(rel_id, "arm", {"units": '["arm/0", "arm/1"]'})

        assert relations.member_units(relation) == ["arm/0", "arm/1"]
        assert relations.member_systems(relation) == [
            MemberSystem(
                unit="arm/0",
                system=PeerSystem(name="arm0", address="10.0.1.1"),
                ready=True,
                service_versions={"lxd": "6.9"},
            )
        ]

    def test_member_systems_lists_one_unit_per_name(self):
        """Two units publishing the same hostname count once, as peers do."""
        orchestrator = _harness()
        rel_id = orchestrator.add_relation("members", "arm")
        for unit in ("arm/0", "arm/1"):
            orchestrator.add_relation_unit(rel_id, unit)
        orchestrator.update_relation_data(rel_id, "arm/1", _member_data("arm0", "10.0.1.2"))
        orchestrator.update_relation_data(rel_id, "arm/0", _member_data("arm0", "10.0.1.1"))
        relations = ClusterRelation(orchestrator.charm)

        systems = relations.member_systems(relations.members_relation(rel_id))

        assert [(member.unit, member.system.address) for member in systems] == [
            ("arm/0", "10.0.1.1")
        ]


def _candidate(name, address="10.0.1.1", *, versions=None, ready=True, initialized=False):
    return MemberSystem(
        unit=f"arm/{name[-1]}",
        system=PeerSystem(name=name, address=address, initialized=initialized),
        ready=ready,
        service_versions=versions or {"lxd": "6.9"},
    )


class TestMemberSessions:
    def _sessions(self, *, members=(), failures=None, current=None):
        unit = MagicMock(spec=ops.Unit)
        unit.status = None
        unit.is_leader.return_value = True
        unit.name = "microcloud/0"
        coordinator = MagicMock()
        coordinator.session.return_value = current
        network = MagicMock(spec=UnitNetwork)
        network.bind_address.return_value = "10.0.0.1"
        relation = MagicMock()
        relation.id = 7
        relation.app.name = "arm"
        relations = MagicMock(spec=ClusterRelation)
        relations.members_relations.return_value = [relation]
        relations.members_relation.return_value = relation
        relations.member_systems.return_value = list(members)
        relations.published_service_versions.return_value = {"lxd": "6.9"}
        relations.failures.return_value = failures or JoinFailures()

        def hold(status):
            unit.status = status

        sessions = session.JoinSessions(
            unit=unit,
            coordinator=coordinator,
            network=network,
            config={"session-timeout": 300},
            charm_dir=Path("/charm"),
            stored=SimpleNamespace(session_failures=0, worker_pid=0, worker_session=""),
            hold_status=hold,
            relations=relations,
        )
        sessions._worker_running = MagicMock(return_value=False)
        sessions._start_worker = MagicMock()
        sessions.pending = MagicMock(return_value=[])
        sessions._preseed_inputs = MagicMock(
            side_effect=lambda address, passphrase, entries: [entry.name for entry in entries]
        )
        return sessions, relations, relation

    def test_candidates_are_ready_new_units_on_the_published_versions(self):
        members = [
            _candidate("arm0"),
            _candidate("arm1", ready=False),
            _candidate("arm2", versions={"lxd": "6.8"}),
            _candidate("arm3", initialized=True),
            _candidate("arm4"),
        ]
        sessions, _, relation = self._sessions(members=members)

        candidates, _ = sessions.member_candidates(relation, {"node0", "arm4"})

        assert [member.system.name for member in candidates] == ["arm0"]

    def test_no_candidates_before_the_orchestrator_published_its_versions(self):
        sessions, relations, relation = self._sessions(members=[_candidate("arm0")])
        relations.published_service_versions.return_value = {}

        assert sessions.member_candidates(relation, set())[0] == []

    def test_failures_exclude_the_same_candidates_and_reset_for_new_ones(self):
        members = [_candidate("arm0")]
        failures = JoinFailures(count=3, error="boom", candidates=candidate_signatures(members))
        sessions, relations, relation = self._sessions(members=members, failures=failures)

        assert sessions.member_candidates(relation, set()) == ([], failures)

        relations.member_systems.return_value = [_candidate("arm0", address="10.0.1.9")]
        candidates, reset = sessions.member_candidates(relation, set())
        assert len(candidates) == 1
        assert reset.count == 0

    @pytest.mark.parametrize(
        "change",
        [
            {"ovn_uplink_interface": "eth9"},
            {"ovn_underlay_ip": "10.9.9.9"},
            {"storage_local_path": "/dev/sdb"},
            {"storage_ceph_paths": ["/dev/sdc"]},
            {"storage_wipe": True},
            {"storage_encrypt": True},
        ],
    )
    def test_fixing_a_candidate_resets_its_failures(self, change):
        members = [_candidate("arm0")]
        failures = JoinFailures(count=3, error="boom", candidates=candidate_signatures(members))
        sessions, relations, relation = self._sessions(members=members, failures=failures)

        fixed = _candidate("arm0")
        for name, value in change.items():
            setattr(fixed.system, name, value)
        relations.member_systems.return_value = [fixed]

        candidates, reset = sessions.member_candidates(relation, set())
        assert [member.system.name for member in candidates] == ["arm0"]
        assert reset == JoinFailures(candidates=candidate_signatures([fixed]))

    def test_a_listed_unit_withdrawing_ready_keeps_the_failures(self):
        """A unit that keeps dropping out must not keep the limit from being reached."""
        both = [_candidate("arm0"), _candidate("arm1", address="10.0.1.2")]
        failures = JoinFailures(count=2, error="boom", candidates=candidate_signatures(both))
        unready = _candidate("arm1", address="10.0.1.2", ready=False)
        sessions, _, relation = self._sessions(members=[both[0], unready], failures=failures)

        candidates, kept = sessions.member_candidates(relation, set())

        assert [member.system.name for member in candidates] == ["arm0"]
        assert kept == failures

    def test_a_listed_unit_with_a_daemon_down_keeps_the_failures(self):
        """Its partial service versions must not read as a unit that changed."""
        both = [_candidate("arm0"), _candidate("arm1", address="10.0.1.2")]
        failures = JoinFailures(count=2, error="boom", candidates=candidate_signatures(both))
        down = _candidate("arm1", address="10.0.1.2", ready=False)
        down.service_versions = {}
        sessions, _, relation = self._sessions(members=[both[0], down], failures=failures)

        candidates, kept = sessions.member_candidates(relation, set())

        assert [member.system.name for member in candidates] == ["arm0"]
        assert kept == failures

    def test_a_listed_unit_returning_changed_resets_the_failures(self):
        both = [_candidate("arm0"), _candidate("arm1", address="10.0.1.2")]
        failures = JoinFailures(count=3, error="boom", candidates=candidate_signatures(both))
        moved = _candidate("arm1", address="10.0.1.9")
        sessions, _, relation = self._sessions(members=[both[0], moved], failures=failures)

        candidates, reset = sessions.member_candidates(relation, set())

        assert len(candidates) == 2
        assert reset.count == 0

    def test_exclusion_holds_while_only_some_listed_units_stay_ready(self):
        both = [_candidate("arm0"), _candidate("arm1", address="10.0.1.2")]
        failures = JoinFailures(count=3, error="boom", candidates=candidate_signatures(both))
        unready = _candidate("arm1", address="10.0.1.2", ready=False)
        sessions, _, relation = self._sessions(members=[both[0], unready], failures=failures)

        assert sessions.member_candidates(relation, set()) == ([], failures)

    def test_removing_a_listed_unit_resets_the_failures(self):
        """Removing the broken unit gives the units that remain another chance."""
        both = [_candidate("arm0"), _candidate("arm1", address="10.0.1.2")]
        failures = JoinFailures(count=3, error="boom", candidates=candidate_signatures(both))
        sessions, _, relation = self._sessions(members=[both[0]], failures=failures)

        candidates, reset = sessions.member_candidates(relation, set())

        assert [member.system.name for member in candidates] == ["arm0"]
        assert reset == JoinFailures(candidates=candidate_signatures([both[0]]))

    def test_clustered_leader_opens_one_session_per_members_relation(self):
        sessions, relations, relation = self._sessions(members=[_candidate("arm0")])

        with (
            patch("session.microcloud.list_members", return_value=[]),
            patch("session.snap.is_installed", return_value=True),
            patch("session.render", side_effect=lambda inputs: inputs),
            patch("session.time.time", return_value=1000.0),
        ):
            problem = sessions.lead(True, [], "secret")

        assert problem is None
        _, document = sessions._start_worker.call_args.args
        assert document == ["arm0"]
        opened = sessions._coordinator.publish_session.call_args.args[0]
        assert opened.relation_id == 7
        assert opened.systems == ["arm0"]
        relations.publish_session.assert_called_once_with(relation, opened)
        assert "Joining 1 unit(s) of arm" in sessions._unit.status.message

    def _lead(self, sessions, pending, *, outcome=None):
        sessions.pending.return_value = pending
        with (
            patch("session.microcloud.list_members", return_value=[]),
            patch("session.microcloud.is_initialized", return_value=True),
            patch("session.microcloud.hostname", return_value="node0"),
            patch("session.snap.is_installed", return_value=True),
            patch("session.render", side_effect=lambda inputs: inputs),
            patch("session.result", return_value=outcome),
            patch("session.clear"),
            patch("session.time.time", return_value=1000.0),
        ):
            return sessions.lead(True, pending, "secret")

    def test_a_peer_that_is_not_ready_does_not_hold_member_units_up(self):
        sessions, _, _ = self._sessions(members=[_candidate("arm0")])
        sessions._coordinator.all_ready.return_value = False

        assert self._lead(sessions, [PeerSystem("node1", "10.0.0.2")]) is None

        opened = sessions._coordinator.publish_session.call_args.args[0]
        assert (opened.relation_id, opened.systems) == (7, ["arm0"])

    def test_a_peer_that_is_not_ready_is_waited_for_with_no_member_candidates(self):
        sessions, _, _ = self._sessions()
        sessions._coordinator.all_ready.return_value = False

        assert self._lead(sessions, [PeerSystem("node1", "10.0.0.2")]) is None

        sessions._start_worker.assert_not_called()
        assert sessions._unit.status.message == "Waiting for all peers to be ready"

    def test_peers_that_are_ready_are_listed_before_member_units(self):
        sessions, _, _ = self._sessions(members=[_candidate("arm0")])

        assert self._lead(sessions, [PeerSystem("node1", "10.0.0.2")]) is None

        opened = sessions._coordinator.publish_session.call_args.args[0]
        assert (opened.relation_id, opened.systems) == (None, ["node1"])

    def test_peer_sessions_that_keep_failing_do_not_hold_member_units_up(self):
        """Neither in the hook that counts the last failure, nor in later ones."""
        pending = [PeerSystem("node1", "10.0.0.2")]
        current = JoinSession("abc", "10.0.0.1", ["node1"], 2000.0)
        sessions, _, _ = self._sessions(members=[_candidate("arm0")], current=current)
        sessions._stored.session_failures = 2

        assert self._lead(sessions, pending, outcome=(1, "Error: boom\n")) is None

        assert sessions._stored.session_failures == 3
        opened = sessions._coordinator.publish_session.call_args.args[0]
        assert (opened.relation_id, opened.systems) == (7, ["arm0"])

        sessions, _, _ = self._sessions(members=[_candidate("arm0")])
        sessions._stored.session_failures = 3

        assert self._lead(sessions, pending) is None

        opened = sessions._coordinator.publish_session.call_args.args[0]
        assert (opened.relation_id, opened.systems) == (7, ["arm0"])

    def test_peer_sessions_that_keep_failing_are_retried_with_no_member_candidates(self):
        sessions, _, _ = self._sessions()
        sessions._stored.session_failures = 3

        assert self._lead(sessions, [PeerSystem("node1", "10.0.0.2")]) is None

        opened = sessions._coordinator.publish_session.call_args.args[0]
        assert (opened.relation_id, opened.systems) == (None, ["node1"])

    def test_a_member_session_keeps_the_storage_options_its_units_published(self):
        """They are that application's options, not the orchestrator's."""
        member = _candidate("arm0")
        member.system.storage_encrypt = True
        sessions, _, _ = self._sessions(members=[member])
        sessions._config = {"session-timeout": 300, "storage-wipe": True}
        sessions._preseed_inputs = MagicMock(side_effect=lambda *args: args[2])

        assert self._lead(sessions, []) is None

        [entry] = sessions._start_worker.call_args.args[1]
        assert (entry.name, entry.storage_wipe, entry.storage_encrypt) == ("arm0", False, True)

    def test_members_relation_needs_microceph(self):
        sessions, _, _ = self._sessions(members=[_candidate("arm0")])

        with (
            patch("session.microcloud.list_members", return_value=[]),
            patch("session.snap.is_installed", return_value=False),
        ):
            problem = sessions.lead(True, [], "secret")

        assert problem == "members relation needs MicroCeph (snap-channel-microceph)"
        sessions._start_worker.assert_not_called()

    @pytest.mark.parametrize(
        ("outcome", "count"),
        [((1, "Error: boom\n"), 2), ((0, "ok\n"), 0), (None, 2)],
    )
    def test_member_session_outcome_counts_against_its_relation(self, outcome, count):
        current = JoinSession("abc", "10.0.0.1", ["arm0"], 2000.0, relation_id=7)
        sessions, relations, relation = self._sessions(
            current=current, failures=JoinFailures(1, "earlier", {"arm0": "sig"})
        )

        with (
            patch("session.result", return_value=outcome),
            patch("session.clear"),
            patch("session.microcloud.is_initialized", return_value=True),
            patch("session.microcloud.list_members", return_value=[]),
        ):
            problem = sessions.lead(True, [], "secret")

        assert problem is None
        assert sessions._stored.session_failures == 0
        relations.publish_session.assert_called_with(relation, None)
        recorded = relations.record_failures.call_args_list[0].args[1]
        assert recorded.count == count
        if count:
            assert recorded.candidates == {"arm0": "sig"}
            assert recorded.error == (
                "Error: boom" if outcome else "the join session ended without a result"
            )

    @pytest.mark.parametrize(
        ("earlier", "problem"),
        [
            (0, None),
            (2, "Join session failed: the join session ended without a result"),
        ],
    )
    def test_an_orchestrator_session_with_no_result_counts_as_failed(self, earlier, problem):
        current = JoinSession("abc", "10.0.0.1", ["node1"], 2000.0)
        sessions, _, _ = self._sessions(current=current)
        sessions._stored.session_failures = earlier

        with (
            patch("session.result", return_value=None),
            patch("session.clear"),
            patch("session.microcloud.is_initialized", return_value=True),
            patch("session.microcloud.list_members", return_value=[]),
        ):
            assert sessions.lead(True, [], "secret") == problem

        assert sessions._stored.session_failures == earlier + 1

    def test_member_renders_only_its_own_entry_with_the_orchestrator_settings(self):
        sessions, _, _ = self._sessions()
        facts = OrchestratorFacts(
            snap_channels={"lxd": "6/stable", "microceph": "tentacle/stable", "microovn": ""},
            session=JoinSession("abc", "10.0.0.1", ["arm0", "arm1"], 2000.0, relation_id=7),
            session_timeout=600,
        )
        own = PeerSystem(name="arm0", address="10.0.1.1")

        with (
            patch("session.render", side_effect=lambda inputs: inputs),
            patch("session.result", return_value=None),
            patch("session.microcloud.hostname", return_value="arm0"),
        ):
            problem = sessions.join_member("secret", facts, own)

        assert problem is None
        _, inputs = sessions._start_worker.call_args.args
        assert [system.name for system in inputs.systems] == ["arm0"]
        assert inputs.initiator_address == "10.0.0.1"
        assert inputs.session_passphrase == session.session_passphrase("secret", "abc")
        assert inputs.session_timeout == 600
        assert inputs.with_ceph and not inputs.with_ovn

    def test_a_members_storage_options_reach_the_initiators_document(self):
        """The initiator's document decides how a member's disks are set up."""
        from preseed import PreseedInputs, build_preseed

        data = identity_fields(
            "arm0",
            "10.0.1.1",
            storage_ceph_paths=["/dev/sdb"],
            storage_wipe=False,
            storage_encrypt=True,
        )
        member = MemberSystem("arm/0", system_from_data(data))
        own = identity_fields(
            "node0", "10.0.0.1", storage_ceph_paths=["/dev/sdc"], storage_wipe=True
        )

        systems = session._system_entries([system_from_data(own), member.system])
        doc = build_preseed(PreseedInputs("10.0.0.1", "secret", systems))

        assert doc["systems"][0]["storage"]["ceph"] == [{"path": "/dev/sdc", "wipe": True}]
        assert doc["systems"][1]["storage"]["ceph"] == [{"path": "/dev/sdb", "encrypt": True}]

    def test_the_members_are_read_once_per_hook(self):
        sessions, _, relation = self._sessions(members=[_candidate("arm0")])

        with (
            patch("session.microcloud.list_members", return_value=[]) as list_members,
            patch("session.snap.is_installed", return_value=True),
            patch("session.render", side_effect=lambda inputs: inputs),
        ):
            assert sessions.has_member_candidates()
            assert sessions.lead(True, [], "secret") is None

        list_members.assert_called_once()

    def test_the_members_are_read_again_once_a_session_has_ended(self):
        current = JoinSession("abc", "10.0.0.1", ["arm0"], 2000.0, relation_id=7)
        sessions, _, _ = self._sessions(current=current)
        sessions.pending = session.JoinSessions.pending.__get__(sessions)
        sessions._coordinator.pending_systems.return_value = []
        member = SimpleNamespace(name="arm0")

        with (
            patch("session.result", return_value=(0, "ok")),
            patch("session.clear"),
            patch("session.microcloud.is_initialized", return_value=True),
            patch("session.microcloud.list_members", return_value=[]) as list_members,
        ):
            sessions.member_names()
            list_members.return_value = [member]
            assert sessions.lead(True, [], "secret") is None

        sessions._coordinator.pending_systems.assert_called_with({"arm0"})

    def test_member_not_listed_waits_for_the_next_session(self):
        sessions, _, _ = self._sessions()
        facts = OrchestratorFacts(session=JoinSession("abc", "10.0.0.1", ["arm1"], 2000.0))

        with patch("session.microcloud.hostname", return_value="arm0"):
            sessions.join_member("secret", facts, PeerSystem("arm0", "10.0.1.1"))

        sessions._start_worker.assert_not_called()
        assert sessions._unit.status.message == "Waiting for the next join session"


class TestJoinSessionData:
    def test_relation_id_round_trips(self):
        opened = JoinSession("abc", "10.0.0.1", ["arm0"], 2000.0, relation_id=7)
        assert JoinSession.from_json(opened.to_json()) == opened

    def test_malformed_data_is_ignored(self):
        assert JoinSession.from_json("{") is None
        assert JoinSession.from_json('{"id": "abc"}') is None


class TestServiceVersions:
    def test_version_mismatches(self):
        own = {"lxd": "6.9", "microceph": "20.1"}
        wanted = {"lxd": "6.9", "microceph": "20.2", "microovn": "26"}
        assert snap.version_mismatches(own, wanted) == {
            "microceph": "20.1",
            "microovn": "unknown",
        }

    def test_tracked_channels_reads_snapd(self):
        body = {
            "result": [
                {"name": "microceph", "tracking-channel": "squid/stable"},
                {"name": "sideloaded"},
            ]
        }
        with patch("snap._get_json", return_value=body):
            assert snap.tracked_channels() == {"microceph": "squid/stable", "sideloaded": ""}

    @pytest.mark.parametrize(
        "read",
        [{"side_effect": OSError("refused")}, {"return_value": {"type": "error"}}],
    )
    def test_tracked_channels_raises_when_snapd_cannot_be_read(self, read):
        with patch("snap._get_json", **read), pytest.raises(snap.SnapError):
            snap.tracked_channels()

    def test_ensure_installed_leaves_installed_snaps_alone(self):
        with (
            patch("snap.is_installed", side_effect=lambda name: name == "lxd"),
            patch("snap.install") as install,
            patch("snap.hold"),
        ):
            snap.ensure_installed({"lxd": "6/stable", "microcloud": "3/stable", "microovn": ""})

        install.assert_called_once_with("microcloud", "3/stable")


class TestPassphrase:
    def _coordinator(self, secret):
        charm = MagicMock()
        charm.unit.is_leader.return_value = True
        charm.model.get_relation.return_value = MagicMock(data=MagicMock())
        coordinator = ClusterCoordinator(charm)
        coordinator._leader_ensure_secret = MagicMock(return_value=secret)
        return coordinator

    def test_grants_every_relation_while_ensuring_the_secret_once(self):
        secret = MagicMock(id="secret:1")
        coordinator = self._coordinator(secret)
        relations = [SimpleNamespace(id=7), SimpleNamespace(id=8)]

        assert coordinator.grant_passphrase(relations) == {7: "secret:1", 8: "secret:1"}

        coordinator._leader_ensure_secret.assert_called_once()
        assert secret.grant.call_count == 2

    def test_a_failed_grant_only_leaves_out_its_own_relation(self):
        secret = MagicMock(id="secret:1")
        secret.grant.side_effect = [ops.ModelError("denied"), None]
        coordinator = self._coordinator(secret)

        relations = [SimpleNamespace(id=7), SimpleNamespace(id=8)]

        assert coordinator.grant_passphrase(relations) == {8: "secret:1"}

    def test_an_unreadable_secret_grants_nothing(self):
        coordinator = self._coordinator(MagicMock())
        coordinator._leader_ensure_secret.side_effect = ops.ModelError("denied")

        assert coordinator.grant_passphrase([SimpleNamespace(id=7)]) == {}

    def test_no_relations_touch_no_secret(self):
        coordinator = self._coordinator(MagicMock())

        assert coordinator.grant_passphrase([]) == {}
        coordinator._leader_ensure_secret.assert_not_called()

    def test_read_passphrase(self):
        model = MagicMock()
        model.get_secret.return_value.get_content.return_value = {"passphrase": "hunter2"}
        assert read_passphrase(model, "secret:1") == "hunter2"
        assert read_passphrase(model, "") is None

    @pytest.mark.parametrize(
        "error",
        [ops.SecretNotFoundError("gone"), ops.ModelError("denied"), KeyError("passphrase")],
    )
    def test_read_passphrase_is_none_until_the_secret_can_be_read(self, error):
        model = MagicMock()
        model.get_secret.side_effect = error
        assert read_passphrase(model, "secret:1") is None


class TestValidateMembership:
    def test_known_units_may_be_members_without_being_expected(self):
        problems = validate_membership({"node0"}, {"node0", "arm0", "rogue"}, known={"arm0"})
        assert problems == ["MicroCloud member 'rogue' is not deployed as a Juju unit"]
