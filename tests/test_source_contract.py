import hashlib
import json
import unittest

from open_data.client import Client
from open_data.decoding import parse_page, records as decode_records
from open_data.origin import load_scenario, serve


class SourceContractTests(unittest.TestCase):
    def test_domain_observations_preserve_source_distinctions(self):
        scenario = load_scenario()
        records = scenario["source_records"]
        with serve(scenario) as origin:
            client = Client(origin.base_url)
            changed = decode_records(client.fetch("/versions/renamed").body, "text/csv")
            self.assertIn("amount", records[0])
            self.assertNotIn("amount", changed[0])
            self.assertIn("appropriation", changed[0])
            self.assertNotEqual(records[0]["unit"], changed[0]["unit"])
            additive = decode_records(client.fetch("/versions/additive").body, "text/csv")
            self.assertIn("note", additive[0])

    def test_every_record_has_the_declared_source_identity(self):
        scenario = load_scenario()
        for record in scenario["source_records"]:
            for field in scenario["identity_fields"]:
                self.assertIn(field, record)
                self.assertNotEqual(str(record[field]).strip(), "")


if __name__ == "__main__":
    unittest.main()
