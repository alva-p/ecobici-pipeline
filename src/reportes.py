"""Reportes CSV en data/gold/reportes/. Se arman solo desde gold (salvo el 5, que es de calidad)."""
import pandas as pd

from src.config import ANIOS, BRONZE, GOLD, REPORTES, SILVER


def _gold(tabla: str, **kw) -> pd.DataFrame:
    return pd.read_parquet(GOLD / tabla / "data.parquet", **kw)


def viajes_por_mes(fact, dim_fecha):
    return (fact.merge(dim_fecha, on="fecha_key")
            .groupby(["anio", "mes"]).size().reset_index(name="cantidad_viajes"))


def top10_estaciones_por_anio(fact, dim_fecha, dim_estacion):
    conteo = (fact.merge(dim_fecha[["fecha_key", "anio"]], on="fecha_key")
              .groupby(["anio", "id_estacion_origen"]).size().reset_index(name="cantidad_viajes")
              .sort_values(["anio", "cantidad_viajes", "id_estacion_origen"], ascending=[True, False, True]))
    conteo["ranking"] = conteo.groupby("anio").cumcount() + 1
    top = conteo[conteo.ranking <= 10].merge(dim_estacion, left_on="id_estacion_origen", right_on="id_estacion")
    return top[["anio", "ranking", "id_estacion", "nombre_estacion", "cantidad_viajes"]].sort_values(["anio", "ranking"])


def duracion_por_rango_y_genero(fact, dim_usuario):
    # inner join: solo viajes cuyo usuario está en dim_usuario (ver DECISIONES.md, D11)
    return (fact.merge(dim_usuario, on="id_usuario")
            .groupby(["rango_etario", "genero"])
            .agg(cantidad_viajes=("id_viaje", "size"), duracion_promedio_min=("duracion_min", "mean"))
            .round({"duracion_promedio_min": 1}).reset_index())


def viajes_circulares(fact, dim_fecha):
    r = (fact.merge(dim_fecha[["fecha_key", "anio"]], on="fecha_key")
         .groupby("anio").agg(total_viajes=("es_circular", "size"), viajes_circulares=("es_circular", "sum"))
         .reset_index())
    r["porcentaje_circulares"] = (100 * r.viajes_circulares / r.total_viajes).round(1)
    return r


def calidad_por_anio():
    """Excepción a 'solo gold': compara bronze vs silver vs cuarentena. Agrega el desglose por motivo."""
    bronze = {a: pd.read_parquet(BRONZE / "recorridos" / f"anio={a}" / "data.parquet", columns=["_anio"]).shape[0]
              for a in ANIOS}
    silver = pd.read_parquet(SILVER / "viajes" / "data.parquet", columns=["anio_fuente"]).anio_fuente.value_counts()
    q = pd.read_parquet(SILVER / "viajes_cuarentena" / "data.parquet", columns=["anio_fuente", "motivo"])
    motivos = pd.crosstab(q.anio_fuente, q.motivo)
    r = pd.DataFrame({"anio": list(ANIOS)})
    r["filas_bronze"] = r.anio.map(bronze)
    r["filas_silver"] = r.anio.map(silver).fillna(0).astype(int)
    r["filas_cuarentena"] = r.anio.map(q.anio_fuente.value_counts()).fillna(0).astype(int)
    r["porcentaje_cuarentena"] = (100 * r.filas_cuarentena / r.filas_bronze).round(2)
    for m in motivos.columns:
        r[f"cuarentena_{m.lower()}"] = r.anio.map(motivos[m]).fillna(0).astype(int)
    return r


def ejecutar() -> None:
    REPORTES.mkdir(parents=True, exist_ok=True)
    fact = _gold("fact_viajes")
    dim_fecha, dim_estacion, dim_usuario = _gold("dim_fecha"), _gold("dim_estacion"), _gold("dim_usuario")
    reportes = {
        "viajes_por_mes": viajes_por_mes(fact, dim_fecha),
        "top10_estaciones_por_anio": top10_estaciones_por_anio(fact, dim_fecha, dim_estacion),
        "duracion_por_rango_y_genero": duracion_por_rango_y_genero(fact, dim_usuario),
        "viajes_circulares": viajes_circulares(fact, dim_fecha),
        "calidad_por_anio": calidad_por_anio(),
    }
    for nombre, df in reportes.items():
        df.to_csv(REPORTES / f"{nombre}.csv", index=False)
        print(f"  reportes/{nombre}.csv: {len(df)} filas")
