"""Ejecuta el pipeline completo: raw -> bronze -> silver -> gold -> reportes.

Uso:  python main.py

Lo armé para que no haga falta ningún paso a mano: cada capa lee lo que escribió la anterior.
Se puede correr las veces que se quiera; el resultado es siempre el mismo (idempotente, D19).
"""
import sys
import time

from src import bronze, gold, reportes, silver
from src.config import RAW

# El orden importa: cada capa depende de la anterior.
CAPAS = [("Bronze", bronze.ejecutar), ("Silver", silver.ejecutar), ("Gold", gold.ejecutar), ("Reportes", reportes.ejecutar)]


def main() -> None:
    # Antes de arrancar chequeo que estén los 6 archivos. Prefiero un mensaje claro ahora
    # a un FileNotFoundError a mitad del pipeline.
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
