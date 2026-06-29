#!/usr/bin/env python3
"""Verify committed .secrets.baseline matches current repo scan."""
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
BASELINE = REPO / ".secrets.baseline"
EXCLUDE = (".venv/|__pycache__/|\\.git/|node_modules/|\\.pytest_cache/"
           "|\\.ruff_cache/|\\.secrets.baseline")

# Scan
import subprocess
result = subprocess.run(
    ["detect-secrets", "scan", "--all-files", "--exclude-files", EXCLUDE],
    capture_output=True, text=True, check=True,
)
new = json.loads(result.stdout)

# Load committed baseline
with open(BASELINE) as f:
    old = json.load(f)

# Normalize: ignore version and timestamps
for d in (old, new):
    d.pop("version", None)
    d.pop("generated_at", None)

if old == new:
    print("✅ No new secrets detected")
    sys.exit(0)

# Show diff
old_set = {
    (p, s["line_number"], s["type"])
    for p, secs in old["results"].items()
    for s in secs
}
new_set = {
    (p, s["line_number"], s["type"])
    for p, secs in new["results"].items()
    for s in secs
}
added = new_set - old_set
removed = old_set - new_set

if added:
    print(f"⚠️  New secrets detected ({len(added)}):")
    for path, line, stype in sorted(added):
        print(f"  ✚ {path}:{line} — {stype}")
if removed:
    print(f"ℹ️  {len(removed)} baseline entries no longer found (update baseline):")
    for path, line, stype in sorted(removed):
        print(f"  ✖ {path}:{line} — {stype}")

print("\nTo update baseline, run:")
print("  detect-secrets scan --all-files --exclude-files '<PATTERN>' > .secrets.baseline")
sys.exit(1 if added else 0)
