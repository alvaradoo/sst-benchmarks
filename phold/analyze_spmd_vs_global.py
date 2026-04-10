#!/usr/bin/env python3
"""Analyze PHOLD trace logs for SPMD vs Global workload.

This script compares two trace logs, counting specific traced message types per rank.

Example:
  python analyze_spmd_vs_global.py \
    --spmd spmd.txt \
    --global global.txt \
    --out-dir analysis
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path


# Exact message types to track (component + message prefix)
TRACKED_TYPES = [
    "[Device] Device.set_partition",
    "[Device] Device.port created",
    "[Device] Device.__init__",
    "[DeviceGraph] DeviceGraph.__init__ created empty graph",
    "[DeviceGraph] _expand_device done",
    "[DeviceGraph] _expand_device start",
    "[DeviceGraph] _link_other_port expanded-through assembly",
    "[DeviceGraph] add device",
    "[DeviceGraph] check_partition complete",
    "[DeviceGraph] flatten call",
    "[DeviceGraph] flatten expanded",
    "[DeviceGraph] follow_links complete",
    "[DeviceGraph] follow_links start",
    "[DeviceGraph] link created",
    "[DeviceGraph] verify_links start",
    "[DeviceGraph] verify_links complete",
    "[DeviceGraph] prune start",
    "[DeviceGraph] prune complete",
    "[DeviceGraph] follow_links expanding",
    "[phold_dist_ahp] SubGrid.__init__",
    "[phold_dist_ahp] SubGrid.expand complete",
    "[phold_dist_ahp] SubGrid.expand start",
    "[phold_dist_ahp] architecture dispatch",
    "[phold_dist_ahp] starting AHP graph construction",
    "[phold_dist_ahp] startup",
    "[phold_dist_ahp] Node.__init__",
    "[SSTGraph] build complete",
    "[SSTGraph] __build_model complete",
    "[SSTGraph] __build_model component",
    "[SSTGraph] __build_model connect",
    "[SSTGraph] __build_model start",
    "[SSTGraph] _flatten complete",
    "[SSTGraph] _flatten start",
    "[SSTGraph] SSTGraph.__init__",
    "[SSTGraph] build start multi-rank",
]


def parse_log(path: Path) -> tuple[dict[int, dict[str, int]], dict[str, int]]:
    """Parse one trace file into per-rank and total counts for tracked message types."""
    per_rank_counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total_counts: dict[str, int] = defaultdict(int)

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\n")
            # Extract rank from [rank=N] pattern in line
            rank_match = re.search(r"\[rank=(\d+)\]", line)
            if not rank_match:
                continue
            rank = int(rank_match.group(1))
            
            # Check each tracked message type
            for msg_type in TRACKED_TYPES:
                if msg_type in line:
                    per_rank_counts[rank][msg_type] += 1
                    total_counts[msg_type] += 1

    return (
        {r: dict(v) for r, v in per_rank_counts.items()},
        dict(total_counts),
    )


def varying_types_for_per_rank(per_rank_counts: dict[int, dict[str, int]]) -> list[str]:
    """Return tracked message types whose counts are not identical across ranks."""
    ranks = sorted(per_rank_counts.keys())
    if not ranks:
        return []

    varying: list[str] = []
    for msg_type in TRACKED_TYPES:
        values = [per_rank_counts[rank].get(msg_type, 0) for rank in ranks]
        if len(set(values)) > 1:
            varying.append(msg_type)
    return varying


def write_summary_markdown(
    spmd_per_rank: dict[int, dict[str, int]],
    global_per_rank: dict[int, dict[str, int]],
    spmd_total: dict[str, int],
    global_total: dict[str, int],
    out_path: Path,
) -> None:
    """Write markdown summary with per-rank tables for tracked message types."""
    lines: list[str] = ["# SPMD vs Global Trace-Type Summary", ""]

    spmd_varying_types = varying_types_for_per_rank(spmd_per_rank)
    global_varying_types = varying_types_for_per_rank(global_per_rank)

    # Build table for SPMD
    lines.extend([
        "## SPMD Per-Rank Trace-Type Counts",
        "",
    ])
    if spmd_varying_types:
        header = "| Rank | " + " | ".join(spmd_varying_types) + " |"
        sep = "|---:|" + "---:|" * len(spmd_varying_types)
        lines.extend([header, sep])

        for rank in sorted(spmd_per_rank.keys()):
            counts = spmd_per_rank[rank]
            row = [str(rank)] + [str(counts.get(t, 0)) for t in spmd_varying_types]
            lines.append("| " + " | ".join(row) + " |")
    else:
        lines.append("All tracked message-type counts are identical across ranks in SPMD.")

    # Build table for GLOBAL
    lines.extend([
        "",
        "## GLOBAL Per-Rank Trace-Type Counts",
        "",
    ])
    if global_varying_types:
        header = "| Rank | " + " | ".join(global_varying_types) + " |"
        sep = "|---:|" + "---:|" * len(global_varying_types)
        lines.extend([header, sep])

        for rank in sorted(global_per_rank.keys()):
            counts = global_per_rank[rank]
            row = [str(rank)] + [str(counts.get(t, 0)) for t in global_varying_types]
            lines.append("| " + " | ".join(row) + " |")
    else:
        lines.append("All tracked message-type counts are identical across ranks in GLOBAL.")

    # Totals comparison
    lines.extend([
        "",
        "## Totals Comparison",
        "",
        "| Message Type | SPMD Count | GLOBAL Count | Difference |",
        "|---|---:|---:|---:|",
    ])
    
    for msg_type in TRACKED_TYPES:
        spmd_count = spmd_total.get(msg_type, 0)
        global_count = global_total.get(msg_type, 0)
        diff = global_count - spmd_count
        lines.append(f"| {msg_type} | {spmd_count} | {global_count} | {diff} |")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare SPMD vs Global logs for specific message types"
    )
    parser.add_argument("--spmd", required=True, help="Path to spmd.txt log")
    parser.add_argument("--global", dest="global_log", required=True, help="Path to global.txt log")
    parser.add_argument("--out-dir", default="analysis", help="Output directory for report files")
    args = parser.parse_args()

    spmd_path = Path(args.spmd)
    global_path = Path(args.global_log)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    spmd_per_rank, spmd_total = parse_log(spmd_path)
    global_per_rank, global_total = parse_log(global_path)
    spmd_varying_types = varying_types_for_per_rank(spmd_per_rank)
    global_varying_types = varying_types_for_per_rank(global_per_rank)

    # Print summary to console
    print("\n=== SPMD Message Type Totals ===")
    for msg_type in TRACKED_TYPES:
        count = spmd_total.get(msg_type, 0)
        print(f"{msg_type}: {count}")

    print("\n=== GLOBAL Message Type Totals ===")
    for msg_type in TRACKED_TYPES:
        count = global_total.get(msg_type, 0)
        print(f"{msg_type}: {count}")

    # Write markdown summary
    summary_md = out_dir / "spmd_vs_global_summary.md"
    write_summary_markdown(
        spmd_per_rank,
        global_per_rank,
        spmd_total,
        global_total,
        summary_md,
    )

    # Write JSON report
    report = {
        "inputs": {
            "spmd": str(spmd_path),
            "global": str(global_path),
        },
        "tracked_message_types": TRACKED_TYPES,
        "spmd_totals": {t: spmd_total.get(t, 0) for t in TRACKED_TYPES},
        "global_totals": {t: global_total.get(t, 0) for t in TRACKED_TYPES},
        "spmd_per_rank_varying_types": spmd_varying_types,
        "global_per_rank_varying_types": global_varying_types,
        "spmd_per_rank": {str(k): {t: v.get(t, 0) for t in spmd_varying_types} for k, v in sorted(spmd_per_rank.items())},
        "global_per_rank": {str(k): {t: v.get(t, 0) for t in global_varying_types} for k, v in sorted(global_per_rank.items())},
    }

    report_path = out_dir / "spmd_vs_global_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"\n=== Output ===")
    print(f"Markdown: {summary_md}")
    print(f"JSON: {report_path}")


if __name__ == "__main__":
    main()
