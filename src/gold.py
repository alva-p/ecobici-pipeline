"""Capa Gold: modelo estrella (fact_viajes + dim_fecha, dim_estacion, dim_usuario).

Acá no hay limpieza: todo llega validado desde silver. Solo se derivan columnas de negocio.
"""
import pandas as pd

from src.bronze import escribir_parquet
from src.config import GOLD, SILVER

DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]  # sin depender del locale
RANGOS_ETARIOS = [(16, 25, "16-25"), (26, 35, "26-35"), (36, 50, "36-50"), (51, 200, "51+")]


def a_fecha_key(fechas: pd.Series) -> pd.Series:
    """2019-03-15 08:42 -> 20190315 (int)."""
    return (fechas.dt.year * 10_000 + fechas.dt.month * 100 + fechas.dt.day).astype("Int64")


def construir_dim_fecha(viajes: pd.DataFrame) -> pd.DataFrame:
    # Calendario continuo del primer al último día: así un día sin viajes también existe (cuenta 0).
    dias = pd.date_range(viajes.fecha_hora_inicio.min().normalize(), viajes.fecha_hora_inicio.max().normalize(), freq="D")
    d = pd.DataFrame({"fecha": dias})
    return pd.DataFrame({
        "fecha_key": a_fecha_key(d.fecha),
        "fecha": d.fecha,
        "anio": d.fecha.dt.year.astype("Int64"),
        "mes": d.fecha.dt.month.astype("Int64"),
        "dia": d.fecha.dt.day.astype("Int64"),
        "dia_semana": d.fecha.dt.dayofweek.map(dict(enumerate(DIAS))).astype("string"),
        "es_fin_de_semana": d.fecha.dt.dayofweek >= 5,
    })


def construir_dim_estacion(viajes: pd.DataFrame) -> pd.DataFrame:
    apariciones = pd.concat([
        viajes[[f"id_estacion_{p}", f"nombre_estacion_{p}", "anio_fuente"]]
        .set_axis(["id_estacion", "nombre_estacion", "anio"], axis=1)
        for p in ("origen", "destino")
    ])
    conteo = apariciones.value_counts(dropna=False).reset_index(name="n")
    # Nombre del año más reciente que tenga nombre; si en ese año hubo variantes, la más usada.
    nombre = (conteo.dropna(subset=["nombre_estacion"]).sort_values(["anio", "n"], ascending=False)
              .drop_duplicates("id_estacion")[["id_estacion", "nombre_estacion"]])
    anios = conteo.groupby("id_estacion").anio.agg(primer_anio="min", ultimo_anio="max").reset_index()
    dim = anios.merge(nombre, on="id_estacion", how="left")
    dim["nombre_estacion"] = dim.nombre_estacion.fillna("SIN NOMBRE")
    return dim[["id_estacion", "nombre_estacion", "primer_anio", "ultimo_anio"]].sort_values("id_estacion").reset_index(drop=True)


def rango_etario(edad: pd.Series) -> pd.Series:
    r = pd.Series("DESCONOCIDO", index=edad.index, dtype="string")
    for lo, hi, etiqueta in RANGOS_ETARIOS:
        r = r.mask(edad.between(lo, hi).fillna(False), etiqueta)
    return r


def construir_dim_usuario(usuarios: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "id_usuario": usuarios.id_usuario,
        "genero": usuarios.genero,
        "edad": usuarios.edad,
        "rango_etario": rango_etario(usuarios.edad),
        "anio_alta": usuarios.fecha_alta.dt.year.astype("Int64"),
    })


def construir_fact_viajes(viajes: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "id_viaje": viajes.id_viaje,
        "fecha_key": a_fecha_key(viajes.fecha_hora_inicio),
        "hora_inicio": viajes.fecha_hora_inicio.dt.hour.astype("Int64"),
        "id_estacion_origen": viajes.id_estacion_origen,
        "id_estacion_destino": viajes.id_estacion_destino,
        "id_usuario": viajes.id_usuario,
        "duracion_min": (viajes.duracion_seg / 60).round(1).astype("Float64"),
        "es_circular": viajes.id_estacion_origen == viajes.id_estacion_destino,
    })


def ejecutar() -> None:
    viajes = pd.read_parquet(SILVER / "viajes" / "data.parquet")
    usuarios = pd.read_parquet(SILVER / "usuarios" / "data.parquet")

    tablas = {
        "dim_fecha": construir_dim_fecha(viajes),
        "dim_estacion": construir_dim_estacion(viajes),
        "dim_usuario": construir_dim_usuario(usuarios),
        "fact_viajes": construir_fact_viajes(viajes),
    }
    fact, dim_f, dim_e, dim_u = (tablas[k] for k in ("fact_viajes", "dim_fecha", "dim_estacion", "dim_usuario"))

    # Verificaciones del modelo estrella
    assert len(fact) == len(viajes), "fact_viajes debe tener una fila por viaje de silver"
    assert fact.fecha_key.isin(dim_f.fecha_key).all(), "fecha_key sin fila en dim_fecha"
    for c in ("id_estacion_origen", "id_estacion_destino"):
        assert fact[c].isin(dim_e.id_estacion).all(), f"{c} sin fila en dim_estacion"
    for k, pk in (("dim_fecha", "fecha_key"), ("dim_estacion", "id_estacion"), ("dim_usuario", "id_usuario")):
        assert tablas[k][pk].is_unique, f"clave {pk} repetida en {k}"

    con_usuario = fact.id_usuario.notna()
    huerfanos = con_usuario & ~fact.id_usuario.isin(dim_u.id_usuario)
    print(f"  viajes con id_usuario que no está en dim_usuario: {huerfanos.sum():,} "
          f"de {con_usuario.sum():,} con usuario ({huerfanos.sum() / con_usuario.sum():.1%}); "
          f"sin id_usuario (2016): {(~con_usuario).sum():,}")

    for nombre, df in tablas.items():
        escribir_parquet(df, GOLD / nombre / "data.parquet")
        print(f"  gold/{nombre}: {len(df):,} filas")
