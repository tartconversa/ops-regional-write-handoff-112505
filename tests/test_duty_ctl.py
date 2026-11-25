import json
import tempfile
import unittest
from pathlib import Path

from duty_ctl import ControlPlane, run_demo

ROOT = Path(__file__).resolve().parents[1]
MATERIALS = ROOT / "materials"


def demo_plane():
    report = run_demo(MATERIALS, None)
    return report


def material_plane():
    from duty_ctl import load_materials, load_jsonl

    plane = ControlPlane()
    load_materials(plane, MATERIALS)
    timed = []
    for name in ("observations", "traffic"):
        timed.extend((r["observed_ms"], r) for r in load_jsonl(MATERIALS / f"{name}.jsonl"))
    for _, row in sorted(timed, key=lambda x: x[0]):
        plane.handle(row)
    return plane


class DemoTimelineTest(unittest.TestCase):
    def setUp(self):
        self.report = demo_plane()
        self.timeline = self.report["timeline"]

    def decision_for(self, op_id):
        rows = [x for x in self.timeline if x["record"].get("op_id") == op_id]
        return rows

    def test_read_sources_and_waiting_watermarks(self):
        by_op = {
            x["record"]["op_id"]: x["decision"]
            for x in self.timeline
            if x["record"].get("kind") == "read"
        }
        self.assertEqual(by_op["r31"]["served_by"], "east")
        self.assertFalse(by_op["r31"]["replicas"]["west"]["eligible"])
        self.assertEqual(by_op["r32"]["mode"], "history")
        self.assertEqual(by_op["r32"]["served_by"], "east")
        self.assertIn("west", by_op["r32"]["eligible_replicas"])
        # ack 分片仍为 38，但 >= min_history 34；落后分片不能借 task 水位。
        west_ack = by_op["r32"]["replicas"]["west"]["shards"]["ack"]
        self.assertEqual(west_ack["applied"], 38)
        self.assertTrue(west_ack["eligible"])
        self.assertEqual(by_op["r34"]["served_by"], "west")
        self.assertEqual(by_op["r34"]["selection_reason"], "failover_to_visible_replica")

    def test_idempotent_write_lifecycle(self):
        m71 = [x["decision"] for x in self.decision_for("m71")]
        self.assertEqual(m71[0]["decision"], "write_accepted")
        committed = next(d for d in m71 if d["decision"] == "write_committed")
        self.assertEqual(committed["served_write_from"], "east")
        duplicate = next(d for d in m71 if d["decision"] == "return_existing_receipt")
        self.assertFalse(duplicate["admitted"])
        self.assertEqual(duplicate["receipt"]["offsets"], {"task": 119, "ack": 42})

        m70 = [x["decision"] for x in self.decision_for("m70")]
        self.assertEqual(m70[0]["decision"], "write_accepted")
        no_effect = next(d for d in m70 if d["decision"] == "op_no_effect")
        self.assertTrue(no_effect["advice"]["retryable"])
        retry = next(d for d in m70 if d["decision"] == "write_accepted" and d.get("retry_of_logical_op"))
        self.assertEqual(retry["region"], "west")
        self.assertEqual(retry["epoch"], 8)

        m72 = [x["decision"] for x in self.decision_for("m72")]
        self.assertTrue(any(d["decision"] == "write_committed" and d["receipt"]["origin"] == "west" for d in m72))
        later_duplicate = [d for d in m72 if d["decision"] == "return_existing_receipt"]
        self.assertTrue(later_duplicate)

    def test_probe_does_not_prove_fence_and_late_route_is_stale(self):
        plane = ControlPlane()
        # Reconstruct through the public report state is insufficient for seq replay,
        # so run the same materials into an in-memory plane.
        from duty_ctl import load_materials, load_jsonl

        load_materials(plane, MATERIALS)
        timed = []
        for name in ("observations", "traffic"):
            timed.extend((r["observed_ms"], r) for r in load_jsonl(MATERIALS / f"{name}.jsonl"))
        for _, row in sorted(timed, key=lambda x: x[0]):
            plane.handle(row)

        at_11 = plane.status_at(11)
        metro = next(t for t in at_11["tenants"] if t["tenant"] == "metro")
        jobs = next(g for g in metro["groups"] if g["group"] == "jobs")
        t8 = next(t for t in jobs["transitions"] if t["epoch"] == 8)
        fence = next(c for c in t8["checks"] if c["capability"] == "fence")
        self.assertEqual(fence["status"], "waiting")
        self.assertIn("e-batch", fence["missing_subjects"])
        self.assertEqual(fence["probe_down_subjects"], ["e-batch"])

        jobs_now = plane.group_status("metro", "jobs")
        self.assertEqual(jobs_now["write_route"]["region"], "east")
        self.assertEqual(jobs_now["write_route"]["epoch"], 9)
        t9 = next(t for t in jobs_now["transitions"] if t["epoch"] == 9)
        self.assertFalse(t9["write_ready"])
        self.assertTrue(any("m70" in x for x in t9["blockers"]))

        west_late = [
            d for d in plane.state["decisions"]
            if d["decision"].get("recorded") == "route_ack"
            and d["decision"].get("evidence", {}).get("observed_ms") == 58
        ][0]["decision"]
        self.assertIn("write", west_late["stale_capabilities"])


class ControllerRulesTest(unittest.TestCase):
    def setUp(self):
        self.ctl = ControlPlane()
        self.ctl.load_group({
            "tenant": "acme",
            "group": "jobs",
            "members": ["a", "b"],
            "min_history": {"a": 10, "b": 20},
            "write_nodes": {"east": ["e1", "e2"], "west": ["w1", "w2"]},
        })
        for shard, ca, ea, wa in [("a", 12, 12, 10), ("b", 22, 22, 19)]:
            self.ctl.load_opening({
                "tenant": "acme", "shard": shard, "epoch": 3, "owner": "east",
                "confirmed_offset": ca, "east_applied": ea, "west_applied": wa,
            })

    def test_lagging_shard_cannot_borrow_watermark(self):
        result = self.ctl.plan_read("acme", "jobs", op_id="r1", prefer="west")
        self.assertFalse(result["ok"])
        self.assertIsNone(result["served_by"])
        west_b = result["replicas"]["west"]["shards"]["b"]
        self.assertFalse(west_b["eligible"])
        self.assertEqual(west_b["shortfall"], 1)
        self.assertTrue(any("b" in w for w in result["waiting_for"]))
        # 东区已达标，因此不指定 west 时仍可由当前读路由东区服务。
        routed = self.ctl.plan_read("acme", "jobs", op_id="r1b")
        self.assertEqual(routed["served_by"], "east")

        self.ctl.record_observation({
            "observed_ms": 100, "event": "replicated", "tenant": "acme", "group": "jobs",
            "region": "west", "subject": "b", "epoch": 3, "offset": 20,
        })
        result = self.ctl.plan_read("acme", "jobs", op_id="r2", prefer="west")
        self.assertTrue(result["ok"])
        self.assertEqual(result["served_by"], "west")

    def test_partial_receipt_rejected(self):
        with self.assertRaises(Exception):
            self.ctl.record_receipt({
                "kind": "receipt", "op_id": "w1", "tenant": "acme", "group": "jobs",
                "epoch": 3, "origin": "east", "offsets": {"a": 13},
            })

    def test_unknown_write_is_not_accepted_twice_and_late_receipt_resolves(self):
        first = self.ctl.submit_write({
            "observed_ms": 1, "kind": "write", "op_id": "op1", "tenant": "acme",
            "group": "jobs", "epoch": 3, "offsets": {},
        })
        self.assertTrue(first["admitted"])
        second = self.ctl.submit_write({
            "observed_ms": 2, "kind": "write", "op_id": "op1", "tenant": "acme",
            "group": "jobs", "epoch": None, "offsets": {},
        })
        self.assertFalse(second["admitted"])
        self.assertEqual(len(self.ctl._op("acme", "op1")["attempts"]), 1)

        receipt = self.ctl.record_receipt({
            "observed_ms": 3, "kind": "receipt", "op_id": "op1", "tenant": "acme",
            "group": "jobs", "epoch": 3, "origin": "east",
            "offsets": {"a": 13, "b": 23},
        })
        self.assertEqual(receipt["decision"], "write_committed")
        read = self.ctl.plan_read("acme", "jobs", after="op1", op_id="cursor-read")
        self.assertEqual(read["served_by"], "east")

    def test_no_effect_allows_new_attempt_but_pending_does_not(self):
        self.ctl.submit_write({
            "observed_ms": 1, "kind": "write", "op_id": "op2", "tenant": "acme",
            "group": "jobs", "epoch": 3, "offsets": {},
        })
        self.ctl.record_negative_outcome({
            "observed_ms": 2, "kind": "no_effect", "op_id": "op2", "tenant": "acme",
            "group": "jobs", "epoch": 3, "origin": "east",
        })
        retry = self.ctl.submit_write({
            "observed_ms": 3, "kind": "write", "op_id": "op2", "tenant": "acme",
            "group": "jobs", "epoch": None, "offsets": {},
        })
        self.assertTrue(retry["admitted"])
        self.assertEqual(retry["attempt"], 2)

    def test_gateway_closed_blocks_new_writes_but_not_receipts(self):
        self.ctl.record_observation({
            "observed_ms": 4, "event": "gateway_closed", "tenant": "acme", "group": "jobs",
            "region": "east", "subject": "write", "epoch": None, "offset": None,
        })
        blocked = self.ctl.submit_write({
            "observed_ms": 5, "kind": "write", "op_id": "op3", "tenant": "acme",
            "group": "jobs", "epoch": 3, "offsets": {},
        })
        self.assertFalse(blocked["admitted"])
        self.assertEqual(blocked["blockers"][0]["code"], "gateway_not_accepting_new_writes")

        self.ctl.record_observation({
            "observed_ms": 6, "event": "gateway_open", "tenant": "acme", "group": "jobs",
            "region": "east", "subject": "write", "epoch": None, "offset": None,
        })
        retry = self.ctl.submit_write({
            "observed_ms": 7, "kind": "write", "op_id": "op3", "tenant": "acme",
            "group": "jobs", "epoch": 3, "offsets": {},
        })
        self.assertTrue(retry["admitted"])
        self.assertEqual(retry["attempt"], 2)

    def test_late_peer_outcome_unblocks_or_advances_boundary(self):
        plane = material_plane()
        blocked = plane.group_status("metro", "jobs")["effective_flow"]["write"]
        self.assertFalse(blocked["admissible"])

        plane.record_negative_outcome({
            "observed_ms": 70, "kind": "no_effect", "op_id": "m70",
            "tenant": "metro", "group": "jobs", "epoch": 8, "origin": "west",
        })
        opened = plane.group_status("metro", "jobs")["effective_flow"]["write"]
        self.assertTrue(opened["admissible"])

        plane = material_plane()
        plane.record_receipt({
            "observed_ms": 70, "kind": "receipt", "op_id": "m70",
            "tenant": "metro", "group": "jobs", "epoch": 8, "origin": "west",
            "offsets": {"task": 121, "ack": 44},
        })
        read = plane.plan_read("metro", "jobs", after="m70", op_id="late-cursor")
        self.assertEqual(read["served_by"], "west")
        blocked_return = plane.group_status("metro", "jobs")["effective_flow"]["write"]
        self.assertFalse(blocked_return["admissible"])
        self.assertTrue(any("replicated" in json.dumps(b, ensure_ascii=False) for b in blocked_return["blockers"]))

    def test_effective_route_does_not_bypass_unready_write_gate(self):
        plane = material_plane()
        blocked = plane.submit_write({
            "observed_ms": 99, "kind": "write", "op_id": "brand-new", "tenant": "metro",
            "group": "jobs", "epoch": None, "offsets": {},
        })
        self.assertFalse(blocked["admitted"])
        self.assertTrue(any(
            "m70" in json.dumps(block, ensure_ascii=False) for block in blocked["blockers"]
        ))

    def test_outcome_without_epoch_or_region_still_matches_latest_attempt(self):
        self.ctl.submit_write({
            "observed_ms": 1, "kind": "write", "op_id": "op-match", "tenant": "acme",
            "group": "jobs", "epoch": 3, "offsets": {},
        })
        decision = self.ctl.record_negative_outcome({
            "observed_ms": 2, "kind": "no_effect", "op_id": "op-match",
            "tenant": "acme", "group": "jobs",
        })
        self.assertTrue(decision["matched_in_flight_attempt"])
        self.assertEqual(self.ctl._op("acme", "op-match")["attempts"][0]["status"], "no_effect")

    def test_contradictory_receipt_after_negative_outcome_is_flagged(self):
        self.ctl.submit_write({
            "observed_ms": 1, "kind": "write", "op_id": "op-conflict", "tenant": "acme",
            "group": "jobs", "epoch": 3, "offsets": {},
        })
        self.ctl.record_negative_outcome({
            "observed_ms": 2, "kind": "no_effect", "op_id": "op-conflict", "tenant": "acme",
            "group": "jobs", "epoch": 3, "origin": "east",
        })
        receipt = self.ctl.record_receipt({
            "observed_ms": 3, "kind": "receipt", "op_id": "op-conflict", "tenant": "acme",
            "group": "jobs", "epoch": 3, "origin": "east",
            "offsets": {"a": 13, "b": 23},
        })
        self.assertTrue(receipt["conflict"])
        self.assertEqual(receipt["decision"], "receipt_conflicts_with_prior_outcome")
        self.assertEqual(self.ctl._op("acme", "op-conflict")["status"], "outcome_conflict")
        with self.assertRaises(Exception):
            self.ctl.plan_read("acme", "jobs", after="op-conflict", op_id="bad-cursor")

    def test_conflict_latch_survives_later_negative_outcomes(self):
        self.ctl.submit_write({
            "observed_ms": 1, "kind": "write", "op_id": "op-latch", "tenant": "acme",
            "group": "jobs", "epoch": 3, "offsets": {},
        })
        self.ctl.record_negative_outcome({
            "observed_ms": 2, "kind": "no_effect", "op_id": "op-latch", "tenant": "acme",
            "group": "jobs", "epoch": 3, "origin": "east",
        })
        self.ctl.record_receipt({
            "observed_ms": 3, "kind": "receipt", "op_id": "op-latch", "tenant": "acme",
            "group": "jobs", "epoch": 3, "origin": "east",
            "offsets": {"a": 13, "b": 23},
        })
        again = self.ctl.record_negative_outcome({
            "observed_ms": 4, "kind": "rejected", "op_id": "op-latch", "tenant": "acme",
            "group": "jobs", "epoch": 3, "origin": "east",
        })
        self.assertFalse(again["ok"])
        resend = self.ctl.submit_write({
            "observed_ms": 5, "kind": "write", "op_id": "op-latch", "tenant": "acme",
            "group": "jobs", "epoch": None, "offsets": {},
        })
        self.assertFalse(resend["admitted"])
        self.assertEqual(self.ctl._op("acme", "op-latch")["status"], "outcome_conflict")

    def test_later_route_ack_clears_early_connection_failure(self):
        self.ctl.record_observation({
            "observed_ms": 1, "event": "request_handoff", "tenant": "acme", "group": "jobs",
            "region": "west", "subject": None, "epoch": 4, "offset": None,
        })
        self.ctl.record_observation({
            "observed_ms": 2, "event": "connection_failed", "tenant": "acme", "group": "jobs",
            "region": "west", "subject": "write-route", "epoch": 4, "offset": None,
        })
        self.ctl.record_observation({
            "observed_ms": 3, "event": "route_ack", "tenant": "acme", "group": "jobs",
            "region": "west", "subject": "write", "epoch": 4, "offset": None,
        })
        # Fence/replica are not complete in this tiny fixture; connection check itself must not wait.
        view = self.ctl.evaluate_transition("acme", "jobs", 4)
        conn = next(c for c in view["checks"] if c["capability"] == "old_new_connections")
        self.assertEqual(conn["status"], "met")

    def test_restart_preserves_receipt_and_operator_progress(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state.json"
            with ControlPlane(path) as ctl:
                ctl.load_group({
                    "tenant": "acme", "group": "jobs", "members": ["a", "b"],
                    "min_history": {"a": 10, "b": 20},
                    "write_nodes": {"east": ["e1"], "west": ["w1"]},
                })
                for shard, confirmed, east, west in [("a", 12, 12, 12), ("b", 22, 22, 22)]:
                    ctl.load_opening({
                        "tenant": "acme", "shard": shard, "epoch": 3, "owner": "east",
                        "confirmed_offset": confirmed, "east_applied": east, "west_applied": west,
                    })
                ctl.submit_write({
                    "observed_ms": 1, "kind": "write", "op_id": "op9", "tenant": "acme",
                    "group": "jobs", "epoch": 3, "offsets": {},
                })
                ctl.record_receipt({
                    "observed_ms": 2, "kind": "receipt", "op_id": "op9", "tenant": "acme",
                    "group": "jobs", "epoch": 3, "origin": "east",
                    "offsets": {"a": 13, "b": 23},
                })
            with ControlPlane(path) as ctl2:
                read = ctl2.plan_read("acme", "jobs", after="op9", op_id="r9")
                self.assertTrue(read["ok"])
                status = ctl2.group_status("acme", "jobs")
                self.assertEqual(status["watermarks"]["a"]["confirmed_offset"], 13)


if __name__ == "__main__":
    unittest.main()
