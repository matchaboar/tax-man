import sys
from pathlib import Path


# Ensure repository root and package src dirs are on sys.path so tests use local
# workspace modules during development. Always (re)insert to guarantee coverage of
# the path manipulation logic without creating duplicates.
ROOT = Path(__file__).resolve().parents[2]
STRATEGY_SRC = ROOT / "strategy" / "src"
WORKFLOW_SRC = ROOT / "workflow" / "src"

for path in (ROOT, STRATEGY_SRC, WORKFLOW_SRC):
    path_str = str(path)
    if path_str in sys.path:
        sys.path.remove(path_str)
    sys.path.insert(0, path_str)
