import sys
from pathlib import Path


# Ensure repository root and package src dirs are on sys.path so tests use local
# workspace modules during development.
ROOT = Path(__file__).resolve().parents[2]
STRATEGY_SRC = ROOT / "strategy" / "src"
WORKFLOW_SRC = ROOT / "workflow" / "src"

for path in (ROOT, STRATEGY_SRC, WORKFLOW_SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
