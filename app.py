from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from flask import Flask, abort, redirect, render_template, request, send_file, url_for

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from runs import list_run_metadata, load_run_metadata, resolve_run_context, resolve_run_dir, stop_run_from_metadata


PREVIEW_ROW_LIMIT = 25
DOWNLOADABLE_FILENAMES = {
    "leads_master.csv",
    "leads_outreach_ready.csv",
    "run_metadata.json",
    "quality_summary.md",
}
PLACE_EXAMPLES = [
    "New York City",
    "Brooklyn",
    "Queens",
    "Manhattan",
    "Bronx",
    "Staten Island",
    "Yonkers",
    "White Plains",
]

app = Flask(__name__, template_folder="web/templates", static_folder="web/static")


@app.get("/healthz")
def healthz():
    return ("ok", 200)


@app.get("/")
def index():
    recent_runs = list_run_metadata(limit=8)
    return render_template(
        "index.html",
        recent_runs=recent_runs,
        place_examples=PLACE_EXAMPLES,
    )


@app.post("/run")
def submit_run():
    niche_input = request.form.get("niche", "").strip()
    place_input = request.form.get("place", "").strip()
    try:
        context = resolve_run_context(niche_input=niche_input, place_input=place_input)
    except ValueError as error:
        return (
            render_template(
                "error.html",
                title="Unsupported run input",
                message=str(error),
            ),
            400,
        )

    try:
        context.write_metadata(
            started_at=_utc_now(),
            status="running",
        )
        process = _launch_pipeline_subprocess(context)
        context.write_metadata(
            started_at=_utc_now(),
            status="running",
            pipeline_pid=process.pid,
        )
    except Exception as error:
        context.write_metadata(
            started_at=_utc_now(),
            finished_at=_utc_now(),
            status="failed",
            error_summary=f"{type(error).__name__}: {error}",
        )
        return redirect(
            url_for(
                "results",
                niche=context.niche_slug,
                place=context.place_slug,
                run_id=context.run_id,
            )
        )

    return redirect(
        url_for(
            "results",
            niche=context.niche_slug,
            place=context.place_slug,
            run_id=context.run_id,
        )
    )


@app.get("/runs")
@app.get("/runs/<niche>")
@app.get("/runs/<niche>/<place>")
def runs_index(niche: str | None = None, place: str | None = None):
    runs = list_run_metadata(niche_slug=niche, place_slug=place)
    return render_template("runs.html", runs=runs, niche=niche or "", place=place or "")


@app.get("/results/<niche>/<place>/<run_id>")
def results(niche: str, place: str, run_id: str):
    run_dir, metadata = _load_run(niche, place, run_id)
    auto_refresh = str(metadata.get("status", "")).strip() in {"running", "starting"}
    return render_template(
        "results.html",
        metadata=metadata,
        run_dir=run_dir,
        master_preview=_csv_preview(run_dir / "leads_master.csv"),
        outreach_preview=_csv_preview(run_dir / "leads_outreach_ready.csv"),
        quality_summary=_read_text_file(run_dir / "quality_summary.md"),
        active_view="overview",
        auto_refresh=auto_refresh,
    )


@app.get("/results/<niche>/<place>/<run_id>/master")
def results_master(niche: str, place: str, run_id: str):
    run_dir, metadata = _load_run(niche, place, run_id)
    auto_refresh = str(metadata.get("status", "")).strip() in {"running", "starting"}
    return render_template(
        "csv_preview.html",
        metadata=metadata,
        preview=_csv_preview(run_dir / "leads_master.csv", preview_limit=50),
        dataset_label="Master Dataset",
        filename="leads_master.csv",
        active_view="master",
        auto_refresh=auto_refresh,
    )


@app.get("/results/<niche>/<place>/<run_id>/outreach-ready")
def results_outreach_ready(niche: str, place: str, run_id: str):
    run_dir, metadata = _load_run(niche, place, run_id)
    auto_refresh = str(metadata.get("status", "")).strip() in {"running", "starting"}
    return render_template(
        "csv_preview.html",
        metadata=metadata,
        preview=_csv_preview(run_dir / "leads_outreach_ready.csv", preview_limit=50),
        dataset_label="Outreach-Ready Dataset",
        filename="leads_outreach_ready.csv",
        active_view="outreach-ready",
        auto_refresh=auto_refresh,
    )


@app.get("/download/<niche>/<place>/<run_id>/<filename>")
def download_artifact(niche: str, place: str, run_id: str, filename: str):
    if filename not in DOWNLOADABLE_FILENAMES:
        abort(404)

    run_dir, _ = _load_run(niche, place, run_id)
    file_path = run_dir / filename
    if not file_path.exists():
        abort(404)
    return send_file(file_path, as_attachment=True, download_name=filename)


@app.post("/results/<niche>/<place>/<run_id>/stop")
def stop_run(niche: str, place: str, run_id: str):
    _run_dir, metadata = _load_run(niche, place, run_id)
    stop_run_from_metadata(metadata)
    return redirect(url_for("results", niche=niche, place=place, run_id=run_id))


def _load_run(niche: str, place: str, run_id: str) -> tuple[Path, dict[str, object]]:
    run_dir = resolve_run_dir(niche, place, run_id)
    metadata_path = run_dir / "run_metadata.json"
    if not metadata_path.exists():
        abort(404)
    metadata = load_run_metadata(metadata_path)
    if metadata.get("niche_slug") != niche or metadata.get("place_slug") != place or metadata.get("run_id") != run_id:
        abort(404)
    return run_dir, metadata


def _csv_preview(csv_path: Path, *, preview_limit: int = PREVIEW_ROW_LIMIT) -> dict[str, object]:
    if not csv_path.exists():
        return {"exists": False, "columns": [], "rows": [], "total_rows": 0, "preview_rows": 0}

    dataframe = pd.read_csv(csv_path)
    preview = dataframe.head(preview_limit)
    return {
        "exists": True,
        "columns": list(preview.columns),
        "rows": preview.fillna("").astype(str).to_dict(orient="records"),
        "total_rows": len(dataframe),
        "preview_rows": len(preview),
    }


def _read_text_file(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _launch_pipeline_subprocess(context) -> subprocess.Popen:
    command = [
        sys.executable,
        str(SRC_DIR / "run_pipeline.py"),
        "--niche",
        context.niche_input,
        "--place",
        context.place_input,
        "--run-id",
        context.run_id,
        "--output-dir",
        str(context.output_dir),
    ]
    return subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@app.errorhandler(404)
def not_found(_error):
    return (
        render_template(
            "error.html",
            title="Run not found",
            message="The requested run or artifact does not exist on disk.",
        ),
        404,
    )


if __name__ == "__main__":
    app.run(debug=True)
