import unittest
import json
from pathlib import Path

from aistore.sdk.types import Smap


class TestHRW(unittest.TestCase):
    def setUp(self):
        self.scenario_dir = Path("./")
        self.scenario_files = list(self.scenario_dir.glob("scenario-*"))

    def tearDown(self):
        for scenario_file in self.scenario_files:
            scenario_file.unlink(missing_ok=True)

    def test_compare_hrw(self):
        for scenario_file in self.scenario_files:
            with self.subTest(scenario_file=scenario_file):
                self.process_scenario_file(scenario_file)

    def process_scenario_file(self, scenario_file):
        """
        Processes a scenario file by loading its JSON content, parsing the Smap structure,
        and verifying the target node assignments using HRW mapping.

        Args:
            scenario_file (Path): The path to the scenario JSON file.

        Raises:
            AssertionError: If the computed target node does not match the expected target node.
        """
        with open(scenario_file, mode="r", encoding="utf-8") as f:
            scenario = json.load(f)
            smap = Smap.parse_obj(scenario["smap"])
            for uname, expected_tname in scenario["hrw_map"].items():
                snode = smap.get_target_for_object(uname)
                self.assertEqual(
                    snode.daemon_id,
                    expected_tname,
                    msg=f"Mismatch in {scenario_file.name}: {uname}, expected {expected_tname}, got {snode.daemon_id}",
                )
