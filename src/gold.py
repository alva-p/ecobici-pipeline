"""Capa Gold: modelo estrella (fact_viajes + dim_fecha, dim_estacion, dim_usuario).

Una regla que respeté a rajatabla: acá NO hay limpieza. Todo llega validado desde silver.
Si me encontraba corrigiendo algo en gold, era señal de que ese código iba en silver
(eso me pasó con los nombres placeholder de estación, y los moví a silver).
En gold solo se derivan columnas que le dan significado de negocio a los datos.
"""
import pandas as pd

from src.bronze import escribir_parquet
from src.config import GOLD, SILVER

# Los nombres de los días los escribo a mano en vez de usar dt.day_name() con locale español:
# el locale depende de la configuración de cada computadora y en otra máquina podría salir en inglés.
DIAS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
# Rangos del enunciado. Los bordes están incluidos: 25 es "16-25" y 26 es "26-35" (D15).
RANGOS_ETARIOS = [(16, 25, "16-25"), (26, 35, "26-35"), (36, 50, "36-50"), (51, 200, "51+")]


def a_fecha_key(fechas: pd.Series) -> pd.Series:
    """2019-03-15 08:42 -> 20190315. La calculo con aritmética y no con strftime porque es mucho
    más rápida sobre 10 millones de filas, y además da directamente un entero."""
    return (fechas.dt.year * 10_000 + fechas.dt.month * 100 + fechas.dt.day).astype("Int64")


def construir_dim_fecha(viajes: pd.DataFrame) -> pd.DataFrame:
    """Una fila por día, del primer al último día con datos (D13).

    Decidí un calendario CONTINUO (2016-01-01 a 2024-12-31, incluidos los años sin datos) en vez de
    solo los días con viajes. Es lo estándar en un data warehouse: un día sin viajes también existe,
    y un reporte tiene que poder mostrarlo con 0 en vez de saltearlo.
    """
    dias = pd.date_range(viajes.fecha_hora_inicio.min().normalize(), viajes.fecha_hora_inicio.max().normalize(), freq="D")
    d = pd.DataFrame({"fecha": dias})
    return pd.DataFrame({
        "fecha_key": a_fecha_key(d.fecha),
        "fecha": d.fecha,
        "anio": d.fecha.dt.year.astype("Int64"),
        "mes": d.fecha.dt.month.astype("Int64"),
        "dia": d.fecha.dt.day.astype("Int64"),
        "dia_semana": d.fecha.dt.dayofweek.map(dict(enumerate(DIAS))).astype("string"),  # 0 = lunes
        "es_fin_de_semana": d.fecha.dt.dayofweek >= 5,
    })


def construir_dim_estacion(viajes: pd.DataFrame) -> pd.DataFrame:
    """Una fila por estación, juntando las de origen y las de destino.

    Para el nombre aplico la regla del enunciado ("el del año más reciente"), con dos agregados:
      - si en ese año la estación tuvo más de una variante, me quedo con la más usada;
      - salteo los nombres nulos (los placeholders que silver anuló, D5), así una estación que en 2019
        se llamaba "KP1808000368" muestra su nombre real de 2016. Si no tiene ningún nombre válido
        (pasa con 2 estaciones de prueba de 2019), queda como "SIN NOMBRE".
    """
    # Pongo origen y destino en un mismo formato (id, nombre, año) y los apilo.
    apariciones = pd.concat([
        viajes[[f"id_estacion_{p}", f"nombre_estacion_{p}", "anio_fuente"]]
        .set_axis(["id_estacion", "nombre_estacion", "anio"], axis=1)
        for p in ("origen", "destino")
    ])
    # value_counts reduce 20 millones de filas a unas mil combinaciones: después todo es instantáneo.
    conteo = apariciones.value_counts(dropna=False).reset_index(name="n")
    nombre = (conteo.dropna(subset=["nombre_estacion"]).sort_values(["anio", "n"], ascending=False)
              .drop_duplicates("id_estacion")[["id_estacion", "nombre_estacion"]])
    anios = conteo.groupby("id_estacion").anio.agg(primer_anio="min", ultimo_anio="max").reset_index()
    dim = anios.merge(nombre, on="id_estacion", how="left")
    dim["nombre_estacion"] = dim.nombre_estacion.fillna("SIN NOMBRE")
    return dim[["id_estacion", "nombre_estacion", "primer_anio", "ultimo_anio"]].sort_values("id_estacion").reset_index(drop=True)


def rango_etario(edad: pd.Series) -> pd.Series:
    """Edad -> "16-25" / "26-35" / "36-50" / "51+". Sin edad (nula en silver) -> "DESCONOCIDO"."""
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
    """Una fila por viaje válido de silver. Uso los ids naturales como claves, como dice el enunciado."""
    return pd.DataFrame({
        "id_viaje": viajes.id_viaje,
        "fecha_key": a_fecha_key(viajes.fecha_hora_inicio),  # la fecha del viaje es la de INICIO
        "hora_inicio": viajes.fecha_hora_inicio.dt.hour.astype("Int64"),
        "id_estacion_origen": viajes.id_estacion_origen,
        "id_estacion_destino": viajes.id_estacion_destino,
        "id_usuario": viajes.id_usuario,  # puede ser nulo (2016) o no tener par en dim_usuario (D14)
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

    # Verificaciones del enunciado ("Cómo verificar que está bien"), más unicidad de las claves.
    # Si alguna falla, se corta antes de escribir.
    assert len(fact) == len(viajes), "fact_viajes debe tener una fila por viaje de silver"
    assert fact.fecha_key.isin(dim_f.fecha_key).all(), "fecha_key sin fila en dim_fecha"
    for c in ("id_estacion_origen", "id_estacion_destino"):
        assert fact[c].isin(dim_e.id_estacion).all(), f"{c} sin fila en dim_estacion"
    for k, pk in (("dim_fecha", "fecha_key"), ("dim_estacion", "id_estacion"), ("dim_usuario", "id_usuario")):
        assert tablas[k][pk].is_unique, f"clave {pk} repetida en {k}"

    # El enunciado pide contar los viajes cuyo usuario no está en dim_usuario. No es un error:
    # cada archivo de usuarios trae solo las altas de ese año (explicación completa en D14).
    con_usuario = fact.id_usuario.notna()
    huerfanos = con_usuario & ~fact.id_usuario.isin(dim_u.id_usuario)
    print(f"  viajes con id_usuario que no está en dim_usuario: {huerfanos.sum():,} "
          f"de {con_usuario.sum():,} con usuario ({huerfanos.sum() / con_usuario.sum():.1%}); "
          f"sin id_usuario (2016): {(~con_usuario).sum():,}")

    for nombre, df in tablas.items():
        escribir_parquet(df, GOLD / nombre / "data.parquet")
        print(f"  gold/{nombre}: {len(df):,} filas")
