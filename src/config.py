from pathlib import Path

BASE = Path(__file__).resolve().parent.parent / "data"
RAW, BRONZE, SILVER, GOLD = (BASE / c for c in ("raw", "bronze", "silver", "gold"))
REPORTES = GOLD / "reportes"
ANIOS = (2016, 2019, 2024)
