#!/usr/bin/env python3
"""Render a static chart from the measured on-call race experiment report.

Matplotlib is an optional authoring dependency, not an AgentSQL dependency:

    uv run --no-project --with matplotlib python tools/render_on_call_race.py \
        experiments/on-call-race/results-2026-10-30.json \
        --output docs/assets/on-call-race.png

The renderer reads measured counts only. It does not run the experiment or
substitute expected values for missing data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


CASES = (
    (
        "snapshot_reads_with_tool_log",
        "Snapshot reads + tool log",
        "Snapshot observations are not tracked dependencies.",
    ),
    (
        "tracked_branch_reads",
        "Tracked branch reads",
        "Validate the versions read by each proposal.",
    ),
    (
        "tracked_plus_recompute",
        "Tracked reads + recompute",
        "Refresh after recheck; run the leave policy again.",
    ),
    (
        "independent_teams",
        "Independent teams",
        "Tracked reads, separate teams, different inputs.",
    ),
)


def _count(value: Any, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def load_report(path: Path) -> tuple[dict[str, Any], str]:
    """Require the actual experiment schema and complete per-case counts."""
    raw = path.read_bytes()
    report = json.loads(raw)
    if not isinstance(report, dict):
        raise ValueError("The report must be a JSON object")
    if report.get("schema_version") != 1 or report.get("experiment") != "on_call_race":
        raise ValueError("Expected on_call_race schema version 1")
    if report.get("status") != "PASS":
        raise ValueError("Only a completed PASS experiment may be rendered")
    rounds = _count(report.get("rounds_per_case"), "rounds_per_case")
    if rounds == 0:
        raise ValueError("The report must contain at least one measured round per case")
    expected_order = [case[0] for case in CASES]
    if report.get("case_order") != expected_order:
        raise ValueError("The report case order does not match the four experiment cases")
    summaries = report.get("summary")
    if not isinstance(summaries, dict):
        raise ValueError("The report must include measured case summaries")
    for case_id, _, _ in CASES:
        case = summaries.get(case_id)
        if not isinstance(case, dict):
            raise ValueError(f"Missing summary for {case_id}")
        for key in (
            "rounds", "requests", "accepted_proposals", "accepted_approvals",
            "accepted_denials", "rechecks", "recomputations", "coverage_violations",
            "incomplete_publications",
        ):
            _count(case.get(key), f"{case_id}.{key}")
        if case["rounds"] != rounds:
            raise ValueError(f"{case_id} has a different number of rounds")
        if case["coverage_violations"] > rounds:
            raise ValueError(f"{case_id} reports more unsafe rosters than rounds")
        if case["incomplete_publications"]:
            raise ValueError(f"{case_id} has partial file/state publication")
        if case["requests"] != rounds * 2:
            raise ValueError(f"{case_id} must have two leave requests per round")
        if case["accepted_proposals"] != (
            case["accepted_approvals"] + case["accepted_denials"]
        ):
            raise ValueError(f"{case_id} proposal and decision counts do not match")
        orders = case.get("acceptance_orders")
        if not isinstance(orders, dict):
            raise ValueError(f"{case_id} must include the actual acceptance orders")
        alice = _count(orders.get("alice_first"), f"{case_id}.alice_first")
        bob = _count(orders.get("bob_first"), f"{case_id}.bob_first")
        if alice + bob != rounds:
            raise ValueError(f"{case_id} acceptance-order counts do not total its rounds")
    return report, hashlib.sha256(raw).hexdigest()


def render(report_path: Path, output_path: Path, svg_path: Path | None = None) -> None:
    report, digest = load_report(report_path)
    try:
        import matplotlib
        matplotlib.use("Agg")
        from matplotlib import pyplot as plt
        from matplotlib.lines import Line2D
        from matplotlib.patches import FancyBboxPatch
    except ImportError as exc:
        raise SystemExit(
            "Install the optional chart authoring tool with: "
            "uv run --no-project --with matplotlib python tools/render_on_call_race.py --help"
        ) from exc

    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "svg.fonttype": "none",
        "savefig.facecolor": "#F7F4ED",
    })
    ink = "#172526"
    muted = "#5B6667"
    danger = "#B34D31"
    safe = "#157C62"
    fig = plt.figure(figsize=(16, 10), dpi=100, facecolor="#F7F4ED")
    canvas = fig.add_axes((0, 0, 1, 1))
    canvas.set_xlim(0, 1)
    canvas.set_ylim(0, 1)
    canvas.axis("off")

    def text(x: float, y: float, label: str, size: float, **kwargs: Any) -> None:
        canvas.text(
            x, y, label, fontsize=size, color=kwargs.pop("color", ink),
            va="center", transform=canvas.transAxes, **kwargs,
        )

    rounds = report["rounds_per_case"]
    text(0.05, 0.945, "AgentSQL", 19, weight="bold")
    text(0.95, 0.945, "CONCURRENCY EXPERIMENT", 10.5, ha="right", color=muted)
    text(0.05, 0.864, "Two leave approvals. Nobody on call.", 33, weight="bold")
    text(
        0.05, 0.805,
        "Both workers read before either publishes. At least one person must stay on call.",
        17, color=muted,
    )
    text(0.05, 0.734, f"FOUR CASES  /  {rounds:,} ROUNDS EACH", 11, weight="bold")
    text(0.63, 0.734, "ROUNDS WITH COVERAGE FAILURE", 11, weight="bold")

    summaries = report["summary"]
    row_centers = (0.638, 0.49, 0.342, 0.194)
    for (case_id, title, subtitle), y in zip(CASES, row_centers):
        case = summaries[case_id]
        unsafe = case["coverage_violations"]
        color = danger if unsafe else safe
        canvas.add_patch(FancyBboxPatch(
            (0.037, y - 0.062), 0.926, 0.127,
            boxstyle="round,pad=0.006,rounding_size=0.012",
            facecolor="#FFFDFA", edgecolor="#E6E4DC", linewidth=0.8,
            transform=canvas.transAxes,
        ))
        canvas.add_patch(FancyBboxPatch(
            (0.05, y - 0.038), 0.004, 0.078,
            boxstyle="round,pad=0,rounding_size=0.002",
            facecolor=color, edgecolor="none", transform=canvas.transAxes,
        ))
        text(0.067, y + 0.033, title, 19, weight="bold")
        text(0.067, y + 0.001, subtitle, 13, color=muted)
        metrics = (
            f"Approvals {case['accepted_approvals']:,}"
            f"    Rechecks {case['rechecks']:,}"
            f"    Denials {case['accepted_denials']:,}"
        )
        text(0.067, y - 0.032, metrics, 13.5, color=ink)

        bar_x, bar_width, bar_height = 0.63, 0.21, 0.022
        canvas.add_patch(FancyBboxPatch(
            (bar_x, y - bar_height / 2), bar_width, bar_height,
            boxstyle="round,pad=0,rounding_size=0.011",
            facecolor="#E8E9E3", edgecolor="none", transform=canvas.transAxes,
        ))
        if unsafe:
            canvas.add_patch(FancyBboxPatch(
                (bar_x, y - bar_height / 2), bar_width * unsafe / rounds, bar_height,
                boxstyle="round,pad=0,rounding_size=0.011",
                facecolor=color, edgecolor="none", transform=canvas.transAxes,
            ))
        else:
            canvas.plot(
                [bar_x], [y], marker="o", markersize=8, color=color,
                transform=canvas.transAxes,
            )
        text(
            0.939, y + 0.004, f"{unsafe:,}/{rounds:,}",
            21, weight="bold", ha="right", color=color,
        )

    canvas.add_line(Line2D(
        [0.05, 0.95], [0.099, 0.099], color="#CFD3CC", linewidth=1,
        transform=canvas.transAxes,
    ))
    both_orders = all(
        summaries[case_id]["acceptance_orders"]["alice_first"] > 0
        and summaries[case_id]["acceptance_orders"]["bob_first"] > 0
        for case_id, _, _ in CASES
    )
    schedule_note = (
        "Both publish orders exercised." if both_orders else "One forced publish order."
    )
    text(
        0.05, 0.066,
        f"Forced schedules, simulated workers. No LLMs. {schedule_note}",
        12.5, color=ink,
    )
    text(
        0.05, 0.035,
        "Measures this controlled race, not throughput or production reliability.",
        11.5, color=muted,
    )
    text(0.95, 0.035, f"Report SHA256 {digest[:12]}", 10.5, ha="right", color=muted)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=100, metadata={
        "Title": "AgentSQL on-call race experiment",
        "Description": f"Measured report: {report_path.name}; SHA256: {digest}",
    })
    if svg_path is not None:
        svg_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(svg_path, metadata={"Title": "AgentSQL on-call race experiment"})
    plt.close(fig)
    print(f"Rendered {output_path} from {report_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="A completed experiment JSON report")
    parser.add_argument("--output", type=Path, default=Path("docs/assets/on-call-race.png"))
    parser.add_argument("--svg", type=Path, help="Also save an editable SVG chart")
    args = parser.parse_args()
    render(args.report, args.output, args.svg)


if __name__ == "__main__":
    main()
