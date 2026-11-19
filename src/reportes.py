"""Los 5 reportes del enunciado (Tarea 5), en CSV dentro de data/gold/reportes/.

Todos se arman SOLO desde gold, con joins del modelo estrella, porque gold es lo que consumiría un
analista. La única excepción es el reporte 5 (calidad), que por definición tiene que mirar bronze,
silver y cuarentena: el enunciado la autoriza explícitamente.
"""
import pandas as pd

from src.config import ANIOS, BRONZE, GOLD, REPORTES, SILVER


def _gold(tabla: str, **kw) -> pd.DataFrame:
    return pd.read_parquet(GOLD / tabla / "data.parquet", **kw)


def viajes_por_mes(fact, dim_fecha):
    """Reporte 1. El año y el mes salen de dim_fecha (vía fecha_key), no de recalcular la fecha:
    para eso existe la dimensión."""
    return (fact.merge(dim_fecha, on="fecha_key")
            .groupby(["anio", "mes"]).size().reset_index(name="cantidad_viajes"))


def top10_estaciones_por_anio(fact, dim_fecha, dim_estacion):
    """Reporte 2. Cuento viajes INICIADOS (estación de origen) por año y me quedo con los 10 primeros.

    Para que el ranking no cambie entre corridas, los empates se desempatan por id_estacion
    ascendente (D16). El nombre sale de dim_estacion, y gracias a la reconciliación de silver (D3)
    los nombres de 2016 son los correctos.
    """
    conteo = (fact.merge(dim_fecha[["fecha_key", "anio"]], on="fecha_key")
              .groupby(["anio", "id_estacion_origen"]).size().reset_index(name="cantidad_viajes")
              .sort_values(["anio", "cantidad_viajes", "id_estacion_origen"], ascending=[True, False, True]))
    conteo["ranking"] = conteo.groupby("anio").cumcount() + 1  # 1, 2, 3... dentro de cada año
    top = conteo[conteo.ranking <= 10].merge(dim_estacion, left_on="id_estacion_origen", right_on="id_estacion")
    return top[["anio", "ranking", "id_estacion", "nombre_estacion", "cantidad_viajes"]].sort_values(["anio", "ranking"])


def duracion_por_rango_y_genero(fact, dim_usuario):
    """Reporte 3. Uso un inner join: solo cuentan los viajes cuyo usuario está en dim_usuario (D17).

    Lo pensé así porque un viaje sin usuario conocido no tiene ni rango etario ni género, y meterlo
    como "DESCONOCIDO" mezclaría dos cosas distintas: usuarios que no declararon su edad y viajes que
    directamente no se pueden cruzar. 2016 no aparece porque esos viajes no traen id de usuario.
    """
    return (fact.merge(dim_usuario, on="id_usuario")
            .groupby(["rango_etario", "genero"])
            .agg(cantidad_viajes=("id_viaje", "size"), duracion_promedio_min=("duracion_min", "mean"))
            .round({"duracion_promedio_min": 1}).reset_index())


def viajes_circulares(fact, dim_fecha):
    """Reporte 4. es_circular ya viene calculado en fact_viajes, así que acá solo sumo
    (True cuenta como 1)."""
    r = (fact.merge(dim_fecha[["fecha_key", "anio"]], on="fecha_key")
         .groupby("anio").agg(total_viajes=("es_circular", "size"), viajes_circulares=("es_circular", "sum"))
         .reset_index())
    r["porcentaje_circulares"] = (100 * r.viajes_circulares / r.total_viajes).round(1)
    return r


def calidad_por_anio():
    """Reporte 5: la excepción a "solo gold". Compara bronze, silver y cuarentena.

    Además de las columnas pedidas agregué una columna por motivo (D18). El porcentaje total solo
    dice CUÁNTO se rechazó; el desglose dice POR QUÉ, que es lo que sirve para mejorar la fuente.
    """
    # De bronze leo una sola columna: alcanza para contar filas y es casi instantáneo.
    bronze = {a: pd.read_parquet(BRONZE / "recorridos" / f"anio={a}" / "data.parquet", columns=["_anio"]).shape[0]
              for a in ANIOS}
    silver = pd.read_parquet(SILVER / "viajes" / "data.parquet", columns=["anio_fuente"]).anio_fuente.value_counts()
    q = pd.read_parquet(SILVER / "viajes_cuarentena" / "data.parquet", columns=["anio_fuente", "motivo"])
    motivos = pd.crosstab(q.anio_fuente, q.motivo)  # tabla año x motivo
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
    # Cargo gold una sola vez y se lo paso a cada reporte, en vez de que cada uno lo relea del disco.
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
