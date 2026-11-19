"""Rutas y años en un solo lugar: si se agrega un año o se mueve la carpeta de datos, se cambia acá."""
from pathlib import Path

# Las rutas parten de la ubicación de este archivo y no del directorio actual,
# así el pipeline funciona igual sin importar desde dónde se ejecute.
BASE = Path(__file__).resolve().parent.parent / "data"
RAW, BRONZE, SILVER, GOLD = (BASE / c for c in ("raw", "bronze", "silver", "gold"))
REPORTES = GOLD / "reportes"
ANIOS = (2016, 2019, 2024)
