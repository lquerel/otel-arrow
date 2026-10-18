# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Check that incomplete or misdirected controller lifecycle evidence cannot pass."""

import json
from pathlib import Path
import tempfile
import unittest

from runtime_probes import require_finding
from verify import (CONTRACT_CONTROLS, MUTATIONS, PROOF, REPO, KERNEL, mutate_body,
                    require_proved, require_rejected, require_selected_proved)


class VerificationEvidenceTests(unittest.TestCase):
    # Scenario: the verifier succeeds with no selected translation files.
    # Guarantees: neither an empty inventory nor an empty selection counts as verification.
    def test_empty_selection_is_rejected(self):
        for expected in ({}, {"transitions/check_progress": ["vc_check_progress"]}):
            with self.subTest(expected=expected), self.assertRaises(RuntimeError):
                require_proved(0, "Proved (0 files)", Path("unused"), expected)

    # Scenario: translation output has the right file count but targets the wrong function.
    # Guarantees: a success summary cannot substitute unrelated proofs for covered kernels.
    def test_wrong_translation_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "unexpected.coma").write_text("translation")
            with self.assertRaises(RuntimeError):
                require_proved(0, "Proved (1 files)", root, {"expected": ["vc_expected"]})

    # Scenario: a certificate omits or substitutes an obligation within the expected file.
    # Guarantees: every reviewed obligation must be present, even when file counts agree.
    def test_changed_obligations_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "expected.coma").write_text("translation")
            (root / "expected").mkdir()
            for goals in ({}, {"vc_wrong": {"prover": "alt-ergo"}}):
                (root / "expected/proof.json").write_text(json.dumps({"proofs": {"Coma": goals}}))
                with self.subTest(goals=goals), self.assertRaises(RuntimeError):
                    require_proved(0, "Proved (1 files)", root, {"expected": ["vc_expected"]})

    # Scenario: a negative control encounters an unrelated compilation or prover setup error.
    # Guarantees: tool failure and rejection at another function cannot simulate bug detection.
    def test_unrelated_failure_is_rejected(self):
        for output in ("Compilation failed vc_check_progress proof failed",
                       "can not calibrate vc_check_progress proof failed",
                       "proof failed vc_reset_quarantined"):
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                require_rejected(1, output, "check_progress")

    # Scenario: the unmodified target cannot prove within the negative-control search budget.
    # Guarantees: a resource-induced failure cannot make the matching mutant appear detected.
    def test_failed_positive_control_is_rejected(self):
        with self.assertRaises(RuntimeError):
            require_selected_proved(1, "proof failed vc_check_progress", Path("unused"),
                                    {"transitions/check_progress": ["vc_check_progress"]},
                                    "check_progress")

    # Scenario: a single-file success summary names an unrelated producer.
    # Guarantees: proof of another function cannot satisfy a selected positive control.
    def test_wrong_selected_summary_is_rejected(self):
        with self.assertRaises(RuntimeError):
            require_selected_proved(0, "Proved (verif/other.coma)", Path("unused"),
                                    {"transitions/check_progress": ["vc_check_progress"]},
                                    "check_progress")

    # Scenario: covered executable code changes at a negative-control mutation site.
    # Guarantees: every configured mutation selects exactly one current production expression.
    def test_mutation_anchors_are_unique(self):
        for label, function, old, new in MUTATIONS:
            with self.subTest(label=label):
                self.assertNotEqual(mutate_body((REPO / KERNEL).read_text(), function, old, new), (REPO / KERNEL).read_text())

    # Scenario: an expression occurs in both a contract and its executable body.
    # Guarantees: mutations affect only the body and reject ambiguous or missing anchors.
    def test_mutation_scope_and_drift(self):
        source = "#[ensures(flag)] fn check(flag: bool) -> bool { flag }"
        self.assertEqual(mutate_body(source, "check", "flag", "false"),
                         "#[ensures(flag)] fn check(flag: bool) -> bool { false }")
        for body in ("true", "flag || flag"):
            with self.subTest(body=body), self.assertRaises(RuntimeError):
                mutate_body("fn check() { " + body + " }", "check", "flag", "false")

    # Scenario: a contract control drifts from its source or reviewed proof inventory.
    # Guarantees: every producer and consumer is uniquely selected and each weakening has one site.
    def test_contract_controls_select_reviewed_functions(self):
        expected = json.loads((PROOF / "expected-proofs.json").read_text())
        for label, relative, contract, producer, consumer in CONTRACT_CONTROLS:
            with self.subTest(label=label):
                self.assertEqual((REPO / relative).read_text().count(contract), 1)
                for function in (producer, consumer):
                    self.assertEqual(sum(path.endswith("/" + function) for path in expected), 1)


class RuntimeFindingEvidenceTests(unittest.TestCase):
    # Scenario: a runtime probe is filtered out, passes unexpectedly, or fails to compile.
    # Guarantees: no unrelated result can be reported as reproduction of a known runtime finding.
    def test_runtime_finding_requires_executed_failure(self):
        for code, output in ((0, "running 0 tests"), (0, "probe ... ok"),
                             (1, "could not compile probe ... FAILED test result: FAILED diagnostic"),
                             (1, "other ... FAILED test result: FAILED diagnostic")):
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                require_finding(code, output, "probe", "diagnostic")

    # Scenario: the intended runtime case executes and fails at its documented assertion.
    # Guarantees: a correctly identified counterexample is accepted independently of other test output.
    def test_expected_runtime_finding_is_recognized(self):
        require_finding(101, "test probe ... FAILED diagnostic test result: FAILED", "probe", "diagnostic")


if __name__ == "__main__":
    unittest.main()
