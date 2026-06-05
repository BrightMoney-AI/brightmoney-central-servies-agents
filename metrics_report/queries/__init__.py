from pathlib import Path

_UAA = Path(__file__).parent / "uaa"
_DP  = Path(__file__).parent / "dp"


def load_uaa(name: str) -> str:
    """Load a UAA SQL query by name from queries/uaa/<name>.sql."""
    return (_UAA / f"{name}.sql").read_text()


def load_dp(name: str) -> str:
    """Load a Data Platform SQL query by name from queries/dp/<name>.sql."""
    return (_DP / f"{name}.sql").read_text()
