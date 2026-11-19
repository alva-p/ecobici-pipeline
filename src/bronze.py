"""Capa Bronze: copia fiel de data/raw/ en Parquet, todo como texto + metadatos de ingesta."""
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import ANIOS, BRONZE, RAW

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


def leer_crudo(ruta: Path) -> pd.DataFrame:
    """Lee un CSV (o el único CSV dentro de un ZIP) sin interpretar ningún valor.

    keep_default_na=False: un campo vacío queda como "" y no como NaN. Así bronze
    guarda exactamente lo que vino; decidir qué es nulo es trabajo de silver.
    """
    opciones = dict(dtype=str, keep_default_na=False, encoding="utf-8-sig")
    if ruta.suffix != ".zip":
        return pd.read_csv(ruta, **opciones)
    with zipfile.ZipFile(ruta) as zf:
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise ValueError(f"{ruta.name}: se esperaba 1 CSV y hay {csvs}")
        with zf.open(csvs[0]) as f:
            return pd.read_csv(f, **opciones)


def escribir_parquet(df: pd.DataFrame, destino: Path) -> None:
    """Escribe a un temporal y lo renombra: nunca queda un Parquet a medio escribir."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(destino)


def ingestar_archivo(ruta: Path, anio: int, destino: Path) -> int:
    df = leer_crudo(ruta)
    df["_archivo_origen"] = ruta.name
    df["_fecha_ingesta"] = datetime.now().isoformat(timespec="seconds")
    df["_anio"] = str(anio)
    # Sobrescribe siempre el mismo archivo -> idempotente (nunca se agrega al final).
    escribir_parquet(df, destino / f"anio={anio}" / "data.parquet")
    return len(df)


def ejecutar() -> dict:
    conteos = {}
    for tabla, por_anio in ARCHIVOS.items():
        for anio in ANIOS:
            n = ingestar_archivo(RAW / por_anio[anio], anio, BRONZE / tabla)
            conteos[(tabla, anio)] = n
            print(f"  bronze/{tabla} {anio}: {n:,} filas")
    return conteos
