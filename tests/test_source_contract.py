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
            bindings = records[0]["varbinds"]
            self.assertNotEqual(bindings[0]["oid"].rsplit(".", 1)[-1], bindings[1]["oid"].rsplit(".", 1)[-1])
            self.assertEqual(records[1]["error_status"], "tooBig")
            self.assertLess(records[1]["sysUpTime"], records[0]["sysUpTime"])
            self.assertEqual(records[2]["varbinds"][0]["type"], "endOfMibView")

    def test_every_record_has_the_declared_source_identity(self):
        scenario = load_scenario()
        for record in scenario["source_records"]:
            for field in scenario["identity_fields"]:
                self.assertIn(field, record)
                self.assertNotEqual(str(record[field]).strip(), "")


if __name__ == "__main__":
    unittest.main()
