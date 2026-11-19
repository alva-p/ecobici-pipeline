"""Ejecuta el pipeline completo: raw -> bronze -> silver -> gold -> reportes.

Uso:  python main.py
"""
import sys
import time

from src import bronze, gold, reportes, silver
from src.config import RAW

CAPAS = [("Bronze", bronze.ejecutar), ("Silver", silver.ejecutar), ("Gold", gold.ejecutar), ("Reportes", reportes.ejecutar)]


def main() -> None:
    faltan = [f for archivos in bronze.ARCHIVOS.values() for f in archivos.values() if not (RAW / f).exists()]
    if faltan:
        sys.exit(f"Faltan archivos en {RAW}:\n  " + "\n  ".join(faltan) + "\nVer README.md.")
    inicio = time.time()
    for nombre, ejecutar in CAPAS:
        t = time.time()
        print(f"[{nombre}]")
        ejecutar()
        print(f"  ({time.time() - t:.0f} s)")
    print(f"Pipeline completo en {time.time() - inicio:.0f} s")


if __name__ == "__main__":
    main()
