import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for path in [ROOT, ROOT / "workflow" / "src", ROOT / "strategy" / "src"]:
    path_str = str(path)
    if path_str in sys.path:
        sys.path.remove(path_str)
    sys.path.insert(0, path_str)
