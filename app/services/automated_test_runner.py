"""Execução de pytest por suite e persistência do último relatório."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.config import get_settings
from app.testing.catalog import AUTOMATED_TEST_SUITES, AutomatedTestSuite, get_suite

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_RUN_LOCK = threading.Lock()
_IS_RUNNING = False


@dataclass
class SuiteTestFailure:
    test_id: str
    message: str


@dataclass
class SuiteRunResult:
    suite_id: str
    name: str
    domain: str
    status: str
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errors: int = 0
    duration_seconds: float = 0.0
    failures: list[SuiteTestFailure] = field(default_factory=list)
    output_tail: str = ""


@dataclass
class AutomatedTestRunReport:
    started_at: str
    finished_at: str
    duration_seconds: float
    overall_status: str
    running: bool = False
    trigger: str = "manual"
    suites: list[SuiteRunResult] = field(default_factory=list)
    summary: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _state_file_path() -> Path:
    settings = get_settings()
    raw = (settings.automated_tests_state_file or ".cache/automated_tests_state.json").strip()
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def load_last_report() -> AutomatedTestRunReport | None:
    path = _state_file_path()
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        suites: list[SuiteRunResult] = []
        for item in payload.get("suites", []):
            if not isinstance(item, dict):
                continue
            suite_id = str(item.get("suite_id") or "").strip()
            if not suite_id:
                continue
            suites.append(
                SuiteRunResult(
                    suite_id=suite_id,
                    name=str(item.get("name") or suite_id),
                    domain=str(item.get("domain") or ""),
                    status=str(item.get("status") or "unknown"),
                    passed=int(item.get("passed", 0)),
                    failed=int(item.get("failed", 0)),
                    skipped=int(item.get("skipped", 0)),
                    errors=int(item.get("errors", 0)),
                    duration_seconds=float(item.get("duration_seconds", 0)),
                    failures=[
                        SuiteTestFailure(
                            test_id=str(failure.get("test_id") or "test"),
                            message=str(failure.get("message") or ""),
                        )
                        for failure in item.get("failures", [])
                        if isinstance(failure, dict)
                    ],
                    output_tail=str(item.get("output_tail", ""))[:4000],
                )
            )
        return AutomatedTestRunReport(
            started_at=payload.get("started_at", ""),
            finished_at=payload.get("finished_at", ""),
            duration_seconds=float(payload.get("duration_seconds", 0)),
            overall_status=payload.get("overall_status", "unknown"),
            running=bool(payload.get("running", False)),
            trigger=payload.get("trigger", "manual"),
            suites=suites,
            summary={key: int(value) for key, value in (payload.get("summary") or {}).items()},
        )
    except (OSError, json.JSONDecodeError, TypeError, KeyError) as exc:
        logger.warning("Nao foi possivel ler estado de testes automatizados: %s", exc)
        return None


def _save_report(report: AutomatedTestRunReport) -> None:
    path = _state_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def is_automated_test_run_in_progress() -> bool:
    return _IS_RUNNING


def _parse_junit(path: Path, suite: AutomatedTestSuite) -> SuiteRunResult:
    result = SuiteRunResult(
        suite_id=suite.id,
        name=suite.name,
        domain=suite.domain,
        status="passed",
    )
    if not path.is_file():
        result.status = "error"
        result.errors = 1
        result.failures.append(SuiteTestFailure(test_id="junit", message="Arquivo JUnit não gerado."))
        return result

    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        result.status = "error"
        result.errors = 1
        result.failures.append(SuiteTestFailure(test_id="junit", message=str(exc)))
        return result

    if root.tag == "testsuite":
        suites = [root]
    else:
        suites = list(root.findall("testsuite"))

    for testsuite in suites:
        result.duration_seconds += float(testsuite.attrib.get("time", 0) or 0)
        for case in testsuite.findall("testcase"):
            failure = case.find("failure")
            error = case.find("error")
            skipped = case.find("skipped")
            if skipped is not None:
                result.skipped += 1
                continue
            if failure is not None or error is not None:
                node = failure if failure is not None else error
                result.failed += 1 if failure is not None else 0
                result.errors += 1 if error is not None else 0
                test_id = f"{case.attrib.get('classname', '')}.{case.attrib.get('name', '')}".strip(".")
                message = (node.attrib.get("message") or (node.text or "")).strip()
                result.failures.append(SuiteTestFailure(test_id=test_id, message=message[:500]))
                continue
            result.passed += 1

    if result.failed or result.errors:
        result.status = "failed"
    elif result.passed == 0 and result.skipped == 0:
        result.status = "empty"
    return result


def _run_pytest_for_suite(suite: AutomatedTestSuite) -> SuiteRunResult:
    junit_path = _state_file_path().parent / f"pytest_{suite.id}.xml"
    junit_path.parent.mkdir(parents=True, exist_ok=True)
    if junit_path.is_file():
        junit_path.unlink()

    command = [
        sys.executable,
        "-m",
        "pytest",
        *suite.pytest_paths,
        "-q",
        "--tb=short",
        f"--junitxml={junit_path}",
    ]
    if suite.marker:
        command.extend(["-m", suite.marker])

    completed = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    parsed = _parse_junit(junit_path, suite)
    output_tail = (completed.stdout or "") + ("\n" + completed.stderr if completed.stderr else "")
    parsed.output_tail = output_tail.strip()[-4000:]
    if completed.returncode not in (0, 1) and parsed.status == "passed":
        parsed.status = "error"
        parsed.errors = max(parsed.errors, 1)
        parsed.failures.append(
            SuiteTestFailure(
                test_id="pytest",
                message=f"pytest encerrou com código {completed.returncode}",
            )
        )
    return parsed


def _build_summary(suites: list[SuiteRunResult]) -> dict[str, int]:
    summary = {"passed": 0, "failed": 0, "skipped": 0, "errors": 0, "suites_ok": 0, "suites_failed": 0}
    for suite in suites:
        summary["passed"] += suite.passed
        summary["failed"] += suite.failed
        summary["skipped"] += suite.skipped
        summary["errors"] += suite.errors
        if suite.status == "passed":
            summary["suites_ok"] += 1
        else:
            summary["suites_failed"] += 1
    return summary


def run_automated_tests(
    *,
    suite_ids: list[str] | None = None,
    trigger: str = "manual",
) -> AutomatedTestRunReport:
    """Executa suites selecionadas (ou todas) e persiste relatório."""
    global _IS_RUNNING
    selected = [get_suite(sid) for sid in suite_ids] if suite_ids else list(AUTOMATED_TEST_SUITES)
    suites_to_run = [suite for suite in selected if suite is not None]
    if suite_ids and len(suites_to_run) != len(suite_ids):
        missing = sorted(set(suite_ids) - {suite.id for suite in suites_to_run})
        raise ValueError(f"Suite(s) desconhecida(s): {', '.join(missing)}")

    started = datetime.now(timezone.utc)
    running_report = AutomatedTestRunReport(
        started_at=started.isoformat(),
        finished_at="",
        duration_seconds=0.0,
        overall_status="running",
        running=True,
        trigger=trigger,
        suites=[
            SuiteRunResult(
                suite_id=suite.id,
                name=suite.name,
                domain=suite.domain,
                status="running",
            )
            for suite in suites_to_run
        ],
    )
    _save_report(running_report)

    results: list[SuiteRunResult] = []
    for suite in suites_to_run:
        try:
            results.append(_run_pytest_for_suite(suite))
        except subprocess.TimeoutExpired:
            results.append(
                SuiteRunResult(
                    suite_id=suite.id,
                    name=suite.name,
                    domain=suite.domain,
                    status="error",
                    errors=1,
                    failures=[SuiteTestFailure(test_id="timeout", message="Tempo máximo de 120s excedido.")],
                )
            )
        except Exception as exc:
            logger.exception("Falha ao executar suite %s", suite.id)
            results.append(
                SuiteRunResult(
                    suite_id=suite.id,
                    name=suite.name,
                    domain=suite.domain,
                    status="error",
                    errors=1,
                    failures=[SuiteTestFailure(test_id="runner", message=str(exc))],
                )
            )

    finished = datetime.now(timezone.utc)
    summary = _build_summary(results)
    overall = "passed" if summary["suites_failed"] == 0 and summary["suites_ok"] > 0 else "failed"
    if summary["suites_ok"] == 0 and summary["suites_failed"] == 0:
        overall = "empty"

    report = AutomatedTestRunReport(
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_seconds=round((finished - started).total_seconds(), 2),
        overall_status=overall,
        running=False,
        trigger=trigger,
        suites=results,
        summary=summary,
    )
    _save_report(report)
    return report


def run_automated_tests_background(
    *,
    suite_ids: list[str] | None = None,
    trigger: str = "manual",
) -> bool:
    """Dispara execução em thread; retorna False se já houver execução em andamento."""
    global _IS_RUNNING

    def _worker() -> None:
        global _IS_RUNNING
        try:
            run_automated_tests(suite_ids=suite_ids, trigger=trigger)
        finally:
            _IS_RUNNING = False

    with _RUN_LOCK:
        if _IS_RUNNING:
            return False
        _IS_RUNNING = True

    thread = threading.Thread(target=_worker, name="automated-test-runner", daemon=True)
    thread.start()
    return True


def catalog_with_status() -> list[dict[str, Any]]:
    """Catálogo enriquecido com último resultado por suite."""
    report = load_last_report()
    by_id = {suite.suite_id: suite for suite in report.suites} if report else {}
    rows: list[dict[str, Any]] = []
    for suite in AUTOMATED_TEST_SUITES:
        last = by_id.get(suite.id)
        rows.append(
            {
                "id": suite.id,
                "name": suite.name,
                "domain": suite.domain,
                "description": suite.description,
                "marker": suite.marker,
                "pytest_paths": list(suite.pytest_paths),
                "last_status": last.status if last else "never",
                "last_passed": last.passed if last else 0,
                "last_failed": last.failed if last else 0,
                "last_skipped": last.skipped if last else 0,
                "last_duration_seconds": last.duration_seconds if last else 0.0,
                "last_failures": [asdict(item) for item in last.failures] if last else [],
            }
        )
    return rows
