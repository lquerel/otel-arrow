# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Regression checks for false-green verification reports."""

import json
from pathlib import Path
import tempfile
import unittest

from verify import FUNCTIONS, require_proved, require_rejected


class VerificationEvidenceTests(unittest.TestCase):
    # Scenario: the verifier exits successfully without selecting any proof files.
    # Guarantees: an empty verification selection cannot produce a passing report.
    def test_empty_selection_is_rejected(self):
        with self.assertRaises(RuntimeError):
            require_proved(0, "Proved (0 files)", Path("unused"))

    # Scenario: a success summary omits the expected function's proof obligation.
    # Guarantees: a stale or misdirected translation cannot satisfy the evidence check.
    def test_missing_obligation_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for function in FUNCTIONS:
                target = root / "otel_arrow_dfe_rate_bucket_proof_rlib/math" / function
                target.mkdir(parents=True)
                target.with_suffix(".coma").write_text("translation")
                (target / "proof.json").write_text(json.dumps({"proofs": {"Coma": {}}}))
            with self.assertRaises(RuntimeError):
                require_proved(0, "Proved (2 files)", root)

    # Scenario: a mutation command fails because compilation or solver setup is broken.
    # Guarantees: infrastructure failure cannot count as detection of an arithmetic bug.
    def test_infrastructure_failure_is_not_a_rejected_mutation(self):
        for output in ["Compilation failed", "can not calibrate"]:
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                require_rejected(1, f"proof failed vc_nanos_for_rate {output}", "nanos_for_rate")

    # Scenario: a mutation fails proof checking at a different function.
    # Guarantees: negative controls must exercise their intended contract.
    def test_wrong_goal_is_not_a_rejected_mutation(self):
        with self.assertRaises(RuntimeError):
            require_rejected(1, "proof failed vc_next_theoretical_arrival", "nanos_for_rate")
