import unittest

from materials import read_feed


class InputsTest(unittest.TestCase):
    def test_group_offsets_and_in_flight_sources(self):
        groups = {(r["tenant"], r["group"]): set(r["members"]) for r in read_feed("groups")}
        traffic = list(read_feed("traffic"))
        evidence = list(read_feed("observations"))
        for row in traffic:
            self.assertIn((row["tenant"], row["group"]), groups)
            if row["kind"] == "receipt":
                self.assertEqual(set(row["offsets"]), groups[row["tenant"], row["group"]])
        fenced = [r for r in evidence if r["tenant"] == "metro" and r["event"] == "fenced" and r["epoch"] == 7]
        old_receipt = next(r for r in traffic if r["kind"] == "receipt" and r["op_id"] == "m71")
        self.assertEqual({r["subject"] for r in fenced}, {"e-api", "e-batch"})
        self.assertLess(min(r["observed_ms"] for r in fenced), old_receipt["observed_ms"])
        self.assertLess(old_receipt["observed_ms"], max(r["observed_ms"] for r in fenced))
        self.assertTrue(any(r["kind"] == "no_effect" for r in traffic))

