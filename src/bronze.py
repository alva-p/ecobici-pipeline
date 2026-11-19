"""Capa Bronze: copia fiel de data/raw/ en Parquet, todo como texto + metadatos de ingesta.

La regla que me puse para esta capa: NO interpretar nada. Si un dato viene mal en el origen,
en bronze también tiene que estar mal. Así, si más adelante encuentro un error en la limpieza,
puedo reprocesar silver desde acá sin volver a descargar los archivos.
"""
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import ANIOS, BRONZE, RAW

# Los nombres de archivo no siguen un patrón (guion medio en 2016, guion bajo en 2019 y 2024),
# así que preferí listarlos explícitamente en vez de armarlos con un f-string que se rompa.
ARCHIVOS = {
    "recorridos": {
        2016: "recorridos-realizados-2016.zip",
        2019: "recorridos-realizados-2019.zip",
        2024: "recorridos-realizados-2024.zip",
    },
    "usuarios": {
        2016: "usuarios-ecobici-2016.csv",
        2019: "usuarios_ecobici_2019.csv",
        2024: "usuarios_ecobici_2024.csv",
    },
}


def leer_crudo(ruta: Path, **kw) -> pd.DataFrame:
    """Lee un CSV (o el único CSV dentro de un ZIP) sin interpretar ningún valor (D1).

    Tres decisiones que tomé acá:
      - dtype=str: todo como texto, lo pide el enunciado.
      - keep_default_na=False: aunque se use dtype=str, pandas convierte "" y "NA" en nulos.
        Yo quiero que bronze guarde exactamente lo que vino; decidir qué es nulo es trabajo de silver.
      - encoding="utf-8-sig": los CSV de 2024 traen BOM (un carácter invisible al principio).
        Sin esto, la primera columna se llamaría "\\ufeffid_recorrido" en vez de "id_recorrido".
    Los **kw extra van directo a read_csv: los uso en la exploración para leer solo nrows=1000.
    """
    opciones = dict(dtype=str, keep_default_na=False, encoding="utf-8-sig", **kw)
    if ruta.suffix != ".zip":
        return pd.read_csv(ruta, **opciones)
    # Abro el ZIP con zipfile en vez de compression="zip" para poder elegir el archivo.
    # Hoy cada ZIP trae un solo CSV, pero si un día trae dos prefiero que el pipeline
    # falle con un mensaje claro y no que lea el equivocado sin avisar.
    with zipfile.ZipFile(ruta) as zf:
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise ValueError(f"{ruta.name}: se esperaba 1 CSV y hay {csvs}")
        with zf.open(csvs[0]) as f:
            return pd.read_csv(f, **opciones)


def escribir_parquet(df: pd.DataFrame, destino: Path) -> None:
    """Escribe a un archivo temporal y después lo renombra (escritura "atómica").

    Si el proceso se corta a la mitad (Ctrl+C, falta de memoria), nunca queda un Parquet
    a medio escribir con el nombre bueno: queda el anterior entero o el nuevo entero.
    Es una de las cosas que resuelven Delta Lake o Iceberg en un lakehouse real (sección 3.6).
    Esta función la reusan silver y gold.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(destino)


def ingestar_archivo(ruta: Path, anio: int, destino: Path) -> int:
    """La función que sugiere el enunciado, con la lectura cambiada por leer_crudo()."""
    df = leer_crudo(ruta)
    # Metadatos de ingesta. El guion bajo inicial marca que no son datos del origen.
    df["_archivo_origen"] = ruta.name
    df["_fecha_ingesta"] = datetime.now().isoformat(timespec="seconds")
    df["_anio"] = str(anio)
    # Idempotencia: cada año se escribe SIEMPRE en el mismo archivo y lo reemplaza.
    # Nunca se agrega al final, así que correrlo dos veces no duplica nada.
    escribir_parquet(df, destino / f"anio={anio}" / "data.parquet")
    return len(df)


def ejecutar() -> dict:
    conteos = {}
    for tabla, por_anio in ARCHIVOS.items():
        for anio in ANIOS:
            n = ingestar_archivo(RAW / por_anio[anio], anio, BRONZE / tabla)
            conteos[(tabla, anio)] = n
            # Estos números los comparé con los de la Tarea 1: coinciden exactamente con las líneas
            # de cada CSV (menos el encabezado), así que bronze no pierde ni agrega filas.
            print(f"  bronze/{tabla} {anio}: {n:,} filas")
    return conteos
