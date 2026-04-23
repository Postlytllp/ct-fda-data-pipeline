import sys
from pathlib import Path
TOX_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TOX_ROOT))
FIXTURES = Path(__file__).resolve().parent / "fixtures"
