"""Persistência e consulta do histórico de execuções de testes automatizados."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy.orm import Session, joinedload

from app.models import AutomatedTestRun, AutomatedTestSuiteResult
from app.services.automated_test_runner import AutomatedTestRunReport, SuiteRunResult, SuiteTestFailure


def resolve_deploy_revision() -> str | None:
    import os

    for key in ("RENDER_GIT_COMMIT", "GIT_COMMIT", "GITHUB_SHA", "COMMIT_SHA"):
        value = (os.environ.get(key) or "").strip()
        if value:
            return value[:64]
    return None


def _report_from_run(run: AutomatedTestRun) -> AutomatedTestRunReport:
    suites: list[SuiteRunResult] = []
    for row in run.suite_results:
        failures: list[SuiteTestFailure] = []
        if row.failures_json:
            try:
                raw = json.loads(row.failures_json)
                if isinstance(raw, list):
                    failures = [
                        SuiteTestFailure(test_id=str(item.get("test_id", "test")), message=str(item.get("message", "")))
                        for item in raw
                        if isinstance(item, dict)
                    ]
            except json.JSONDecodeError:
                pass
        suites.append(
            SuiteRunResult(
                suite_id=row.suite_id,
                name=row.suite_name,
                domain=row.suite_domain,
                status=row.status,
                passed=row.passed_count,
                failed=row.failed_count,
                skipped=row.skipped_count,
                errors=row.error_count,
                duration_seconds=float(row.duration_seconds or 0),
                failures=failures,
                output_tail=row.output_tail or "",
            )
        )
    summary: dict[str, int] = {}
    if run.summary_json:
        try:
            parsed = json.loads(run.summary_json)
            if isinstance(parsed, dict):
                summary = {key: int(value) for key, value in parsed.items()}
        except json.JSONDecodeError:
            summary = {}
    return AutomatedTestRunReport(
        started_at=run.started_at.isoformat() if run.started_at else "",
        finished_at=run.finished_at.isoformat() if run.finished_at else "",
        duration_seconds=float(run.duration_seconds or 0),
        overall_status=run.overall_status,
        running=bool(run.is_running),
        trigger=run.trigger_source,
        suites=suites,
        summary=summary,
    )


class AutomatedTestHistoryRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_running_run(self) -> AutomatedTestRun | None:
        return (
            self.db.query(AutomatedTestRun)
            .filter(AutomatedTestRun.is_running.is_(True))
            .order_by(AutomatedTestRun.started_at.desc())
            .first()
        )

    def get_latest_completed_run(self) -> AutomatedTestRun | None:
        return (
            self.db.query(AutomatedTestRun)
            .options(joinedload(AutomatedTestRun.suite_results))
            .filter(AutomatedTestRun.is_running.is_(False))
            .order_by(AutomatedTestRun.finished_at.desc(), AutomatedTestRun.id.desc())
            .first()
        )

    def get_latest_report(self) -> AutomatedTestRunReport | None:
        running = self.get_running_run()
        if running:
            return _report_from_run(
                self.db.query(AutomatedTestRun)
                .options(joinedload(AutomatedTestRun.suite_results))
                .filter(AutomatedTestRun.id == running.id)
                .first()
                or running
            )
        completed = self.get_latest_completed_run()
        if completed:
            return _report_from_run(completed)
        return None

    def start_run(
        self,
        *,
        trigger: str,
        environment: str,
        deploy_revision: str | None,
        initiated_by_user_id: int | None,
        suite_placeholders: list[tuple[str, str, str]],
    ) -> AutomatedTestRun:
        run = AutomatedTestRun(
            trigger_source=trigger,
            overall_status="running",
            is_running=True,
            deploy_revision=deploy_revision,
            environment=environment,
            initiated_by_user_id=initiated_by_user_id,
            started_at=datetime.now(timezone.utc),
        )
        self.db.add(run)
        self.db.flush()
        for suite_id, name, domain in suite_placeholders:
            self.db.add(
                AutomatedTestSuiteResult(
                    run_id=run.id,
                    suite_id=suite_id,
                    suite_name=name,
                    suite_domain=domain,
                    status="running",
                )
            )
        self.db.commit()
        self.db.refresh(run)
        return run

    def finalize_run(self, run_id: int, report: AutomatedTestRunReport) -> None:
        run = self.db.get(AutomatedTestRun, run_id)
        if not run:
            return
        finished = datetime.now(timezone.utc)
        run.overall_status = report.overall_status
        run.is_running = False
        run.duration_seconds = report.duration_seconds
        run.summary_json = json.dumps(report.summary, ensure_ascii=False)
        run.finished_at = finished

        by_suite = {suite.suite_id: suite for suite in report.suites}
        for row in list(run.suite_results):
            updated = by_suite.get(row.suite_id)
            if not updated:
                continue
            row.status = updated.status
            row.passed_count = updated.passed
            row.failed_count = updated.failed
            row.skipped_count = updated.skipped
            row.error_count = updated.errors
            row.duration_seconds = updated.duration_seconds
            row.failures_json = json.dumps([{"test_id": f.test_id, "message": f.message} for f in updated.failures], ensure_ascii=False)
            row.output_tail = (updated.output_tail or "")[:4000] or None

        self.db.add(run)
        self.db.commit()

    def list_runs(self, *, limit: int = 30) -> list[AutomatedTestRun]:
        return (
            self.db.query(AutomatedTestRun)
            .options(joinedload(AutomatedTestRun.suite_results))
            .filter(AutomatedTestRun.is_running.is_(False))
            .order_by(AutomatedTestRun.finished_at.desc(), AutomatedTestRun.id.desc())
            .limit(limit)
            .all()
        )

    def suite_success_rates(self, *, run_limit: int = 30) -> dict[str, dict[str, int | float]]:
        runs = self.list_runs(limit=run_limit)
        stats: dict[str, dict[str, int | float]] = {}
        for run in runs:
            for row in run.suite_results:
                bucket = stats.setdefault(
                    row.suite_id,
                    {"total": 0, "passed_runs": 0, "failed_runs": 0, "name": row.suite_name},
                )
                bucket["name"] = row.suite_name
                bucket["total"] += 1
                if row.status == "passed":
                    bucket["passed_runs"] += 1
                else:
                    bucket["failed_runs"] += 1
        for bucket in stats.values():
            total = int(bucket["total"])
            bucket["success_rate"] = round((int(bucket["passed_runs"]) / total) * 100, 1) if total else 0.0
        return stats

    def history_rows_for_template(self, *, limit: int = 30) -> list[dict]:
        rows: list[dict] = []
        for run in self.list_runs(limit=limit):
            summary: dict = {}
            if run.summary_json:
                try:
                    summary = json.loads(run.summary_json)
                except json.JSONDecodeError:
                    summary = {}
            rows.append(
                {
                    "id": run.id,
                    "trigger": run.trigger_source,
                    "status": run.overall_status,
                    "deploy_revision": (run.deploy_revision or "")[:12],
                    "deploy_revision_full": run.deploy_revision or "—",
                    "environment": run.environment,
                    "started_at": run.started_at,
                    "finished_at": run.finished_at,
                    "duration_seconds": float(run.duration_seconds or 0),
                    "passed": int(summary.get("passed", 0)),
                    "failed": int(summary.get("failed", 0)),
                    "suites_ok": int(summary.get("suites_ok", 0)),
                    "suites_failed": int(summary.get("suites_failed", 0)),
                    "suite_count": len(run.suite_results),
                }
            )
        return rows
