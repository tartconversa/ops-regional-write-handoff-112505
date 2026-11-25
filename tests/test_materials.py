import unittest

from materials import list_feeds, read_feed

FIELDS = {
    'groups': ('tenant', 'group', 'members', 'min_history', 'write_nodes'),
    'opening': ('tenant', 'shard', 'epoch', 'owner', 'confirmed_offset', 'east_applied', 'west_applied'),
    'observations': ('observed_ms', 'event', 'tenant', 'group', 'region', 'subject', 'epoch', 'offset'),
    'traffic': ('observed_ms', 'kind', 'op_id', 'tenant', 'group', 'epoch', 'origin', 'offsets'),
}


class MaterialTest(unittest.TestCase):
    def test_feed_fields(self):
        self.assertEqual(list_feeds(), sorted(FIELDS))
        for name, fields in FIELDS.items():
            with self.subTest(feed=name):
                records = list(read_feed(name))
                self.assertTrue(records)
                self.assertTrue(all(set(row) == set(fields) for row in records))
