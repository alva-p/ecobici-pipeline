"""Capa Silver: esquema canónico, tipos correctos, valores normalizados y cuarentena.

Todo lo que cambia entre años vive en los diccionarios MAPEO_* y FORMATO_*.
Si llega un año nuevo con otro formato, solo se toca este archivo.
"""
import unicodedata

import numpy as np
import pandas as pd

from src.bronze import escribir_parquet
from src.config import ANIOS, BRONZE, SILVER

# ---------------------------------------------------------------- esquemas canónicos
ESQUEMA_VIAJES = {
    "id_viaje": "string",
    "fecha_hora_inicio": "datetime64[ns]",
    "fecha_hora_fin": "datetime64[ns]",
    "duracion_seg": "Int64",
    "id_estacion_origen": "Int64",
    "nombre_estacion_origen": "string",
    "id_estacion_destino": "Int64",
    "nombre_estacion_destino": "string",
    "id_usuario": "string",
    "anio_fuente": "Int64",
}
ESQUEMA_USUARIOS = {
    "id_usuario": "string",
    "genero": "string",
    "edad": "Int64",
    "fecha_alta": "datetime64[ns]",
    "anio_fuente": "Int64",
}

# ---------------------------------------------------------------- mapeos por año
# 2016 no trae id de viaje ni id de usuario: no están en el mapeo y se resuelven en
# preparar_viajes(). La duración de 2016 tampoco se mapea (ver DECISIONES.md, D4).
_COMUNES = {
    "fecha_origen_recorrido": "fecha_hora_inicio",
    "fecha_destino_recorrido": "fecha_hora_fin",
    "id_estacion_origen": "id_estacion_origen",
    "nombre_estacion_origen": "nombre_estacion_origen",
    "id_estacion_destino": "id_estacion_destino",
    "nombre_estacion_destino": "nombre_estacion_destino",
}
MAPEO_VIAJES = {
    2016: {**_COMUNES},
    2019: {"Id_recorrido": "id_viaje", "duracion_recorrido": "duracion_seg",
           "id_usuario": "id_usuario", **_COMUNES},
    2024: {"id_recorrido": "id_viaje", "duracion_recorrido": "duracion_seg",
           "id_usuario": "id_usuario", **_COMUNES},
}
FORMATO_FECHA_VIAJES = {a: "%Y-%m-%d %H:%M:%S" for a in ANIOS}

MAPEO_USUARIOS = {
    2016: {"id_usuario": "id_usuario"},
    2019: {"ID_usuario": "id_usuario"},
    2024: {"id_usuario": "id_usuario"},
}
for _m in MAPEO_USUARIOS.values():
    _m.update(genero_usuario="genero", edad_usuario="edad", fecha_alta="fecha_alta", hora_alta="hora_alta")
FORMATO_ALTA = {2016: "%d-%m-%y %I:%M:%S %p", 2019: "%Y-%m-%d %H:%M:%S", 2024: "%Y-%m-%d %H:%M:%S"}

MAPEO_GENERO = {
    "F": "F", "FEMALE": "F", "FEMENINO": "F", "MUJER": "F",
    "M": "M", "MALE": "M", "MASCULINO": "M", "HOMBRE": "M",
    "OTHER": "OTRO", "OTRO": "OTRO", "X": "OTRO", "NO BINARIO": "OTRO",
}  # cualquier otro valor (incluido nulo) -> DESCONOCIDO

VALORES_NULOS = ["", "NA"]  # bronze guarda los vacíos como "" y 2019 trae edades "NA"
EDAD_MIN, EDAD_MAX = 16, 90
# Nombres provisorios/de prueba del sistema 2019 (no son nombres de estación): se guardan como nulo (D5)
NOMBRES_PLACEHOLDER = {"KP1808000368", "ERRONEA", "PLANEADA", "OFF BRAUDACCO", "TEST TECNICO PDM"}
DIST_MAX_RECONCILIACION_M = 300  # ~3 cuadras: coords de 2016 corridas hasta ~270 m (ver D3)


# ---------------------------------------------------------------- helpers
def renombrar(df: pd.DataFrame, mapeo: dict) -> pd.DataFrame:
    return df.rename(columns=mapeo)[list(mapeo.values())]


def a_entero(s: pd.Series) -> pd.Series:
    """'5BAEcobici' -> 5, '992557.0' -> 992557, '2,814' -> 2814, '' -> <NA>."""
    s = s.str.replace("BAEcobici", "", regex=False).str.replace(",", "", regex=False)
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def limpiar_id(s: pd.Series) -> pd.Series:
    """Ids como texto sin sufijos de sistema ni '.0' de float: '6243BAEcobici' -> '6243'."""
    return s.str.replace("BAEcobici", "", regex=False).str.replace(r"\.0$", "", regex=True).astype("string")


def normalizar_nombre(s: pd.Series) -> pd.Series:
    """'005 - Plaza Italia ' -> 'PLAZA ITALIA'; 'Pacífico' -> 'PACIFICO'."""
    s = (s.str.replace(r"^\s*\d+\s*-\s*", "", regex=True)   # prefijo numérico de 2019/2024
          .str.strip().str.upper().str.replace(r"\s+", " ", regex=True))
    sin_tildes = {v: unicodedata.normalize("NFKD", v).encode("ascii", "ignore").decode()
                  for v in s.dropna().unique()}              # pocas estaciones: mapear únicos es rápido
    s = s.map(sin_tildes).astype("string")
    return s.mask(s.isin(NOMBRES_PLACEHOLDER))


def validar_esquema(df: pd.DataFrame, esquema: dict) -> None:
    real = {c: str(t) for c, t in df.dtypes.items()}
    if real != esquema:
        raise TypeError(f"Esquema inválido.\nEsperado: {esquema}\nReal:     {real}")


# ---------------------------------------------------------------- reconciliación de estaciones 2016
def _distancia_m(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6_371_000 * np.arcsin(np.sqrt(a))


def _catalogo_estaciones(anio: int) -> pd.DataFrame:
    """id, nombre normalizado y coordenadas de cada estación de origen de un año (desde bronze).
    Se usan solo las de origen porque en 2019 las coordenadas de destino vienen corridas."""
    cols = ["id_estacion_origen", "nombre_estacion_origen", "lat_estacion_origen", "long_estacion_origen"]
    d = pd.read_parquet(BRONZE / "recorridos" / f"anio={anio}" / "data.parquet", columns=cols)
    d = d.drop_duplicates("id_estacion_origen", keep="last")
    return pd.DataFrame({
        "id": a_entero(d.id_estacion_origen).to_numpy(),
        "nombre": normalizar_nombre(d.nombre_estacion_origen).fillna("").to_numpy(),
        "lat": d.lat_estacion_origen.astype(float).to_numpy(),
        "lon": d.long_estacion_origen.astype(float).to_numpy(),
    })


def reconciliar_estaciones_2016() -> pd.DataFrame:
    """En 2016 el id de estación es el número público; desde 2019 es un id interno.
    Devuelve id_2016 -> id_estacion, con el criterio usado (ver DECISIONES.md, D3)."""
    viejas = _catalogo_estaciones(2016)
    nuevas = pd.concat([_catalogo_estaciones(2024), _catalogo_estaciones(2019)]).drop_duplicates("id")
    dist = _distancia_m(viejas.lat.to_numpy()[:, None], viejas.lon.to_numpy()[:, None],
                        nuevas.lat.to_numpy()[None, :], nuevas.lon.to_numpy()[None, :])
    por_id = nuevas.set_index("id")
    offset = 10 ** len(str(nuevas.id.max()))  # ids "solo 2016" fuera del rango de los nuevos
    filas = []
    for i, v in enumerate(viejas.itertuples()):
        mismo_nombre = (nuevas.nombre.to_numpy() == v.nombre) & (dist[i] < DIST_MAX_RECONCILIACION_M)
        mismo_id_cerca = v.id in por_id.index and dist[i][nuevas.id.to_numpy() == v.id][0] < DIST_MAX_RECONCILIACION_M
        if mismo_nombre.any():
            nuevo, criterio = int(nuevas.id.to_numpy()[mismo_nombre][dist[i][mismo_nombre].argmin()]), "MISMO_NOMBRE_Y_UBICACION"
        elif mismo_id_cerca:
            nuevo, criterio = v.id, "MISMO_ID_Y_UBICACION"
        elif v.id not in por_id.index:
            nuevo, criterio = v.id, "ID_SIN_USO_POSTERIOR"
        else:
            nuevo, criterio = offset + v.id, "ESTACION_SOLO_2016"
        filas.append((v.id, v.nombre, nuevo, por_id.nombre.get(nuevo, v.nombre), criterio))
    return pd.DataFrame(filas, columns=["id_2016", "nombre_2016", "id_estacion", "nombre_actual", "criterio"])


# ---------------------------------------------------------------- viajes
def preparar_viajes(anio: int, mapa_2016: dict) -> pd.DataFrame:
    mapeo = MAPEO_VIAJES[anio]
    df = pd.read_parquet(BRONZE / "recorridos" / f"anio={anio}" / "data.parquet", columns=list(mapeo))
    df = renombrar(df, mapeo).replace(VALORES_NULOS, pd.NA)

    fmt = FORMATO_FECHA_VIAJES[anio]
    for c in ("fecha_hora_inicio", "fecha_hora_fin"):
        df[c] = pd.to_datetime(df[c], format=fmt, errors="coerce").astype("datetime64[ns]")
    for c in ("id_estacion_origen", "id_estacion_destino"):
        df[c] = a_entero(df[c])
        if anio == 2016:
            df[c] = df[c].map(mapa_2016).astype("Int64")
    for c in ("nombre_estacion_origen", "nombre_estacion_destino"):
        df[c] = normalizar_nombre(df[c])

    if "duracion_seg" in df:   # 2019 y 2024: en segundos (2019 con coma de miles: "2,814")
        df["duracion_seg"] = a_entero(df["duracion_seg"])
    else:                      # 2016: se calcula de las fechas (D4)
        seg = (df.fecha_hora_fin - df.fecha_hora_inicio).dt.total_seconds()
        df["duracion_seg"] = seg.round().astype("Int64")

    if "id_viaje" in df:
        df["id_viaje"] = limpiar_id(df["id_viaje"])
    else:                      # 2016: id sintético y determinístico (D2)
        df["id_viaje"] = ("2016-" + pd.Series(np.arange(1, len(df) + 1), index=df.index).astype(str)).astype("string")
    df["id_usuario"] = limpiar_id(df["id_usuario"]) if "id_usuario" in df else pd.Series(pd.NA, index=df.index, dtype="string")
    df["anio_fuente"] = pd.Series(anio, index=df.index, dtype="Int64")
    return df[list(ESQUEMA_VIAJES)]


def aplicar_calidad(df: pd.DataFrame):
    """Devuelve (validos, cuarentena). Cada fila rechazada guarda el primer motivo que falló."""
    motivo = pd.Series(pd.NA, index=df.index, dtype="string")
    reglas = [
        ("FECHA_INVALIDA", df.fecha_hora_inicio.isna() | df.fecha_hora_fin.isna()),
        ("ESTACION_NULA", df.id_estacion_origen.isna() | df.id_estacion_destino.isna()),
        ("FIN_ANTERIOR_A_INICIO", df.fecha_hora_fin < df.fecha_hora_inicio),
        ("DURACION_MAYOR_24H", df.duracion_seg > 86_400),
        ("DUPLICADO", df.id_viaje.duplicated(keep="first")),
    ]
    for nombre, condicion in reglas:
        motivo = motivo.mask(motivo.isna() & condicion.fillna(False), nombre)
    validos = df[motivo.isna()]
    cuarentena = df[motivo.notna()].assign(motivo=motivo[motivo.notna()])
    return validos, cuarentena


# ---------------------------------------------------------------- usuarios
def preparar_usuarios(anio: int) -> pd.DataFrame:
    mapeo = MAPEO_USUARIOS[anio]
    df = pd.read_parquet(BRONZE / "usuarios" / f"anio={anio}" / "data.parquet", columns=list(mapeo))
    df = renombrar(df, mapeo).replace(VALORES_NULOS, pd.NA)
    df["id_usuario"] = limpiar_id(df["id_usuario"])
    df["genero"] = df["genero"].str.strip().str.upper().map(MAPEO_GENERO).fillna("DESCONOCIDO").astype("string")
    edad = pd.to_numeric(df["edad"], errors="coerce").astype("Int64")
    df["edad"] = edad.where(edad.between(EDAD_MIN, EDAD_MAX))  # fuera de rango -> nulo (D7)
    df["fecha_alta"] = pd.to_datetime(df.fecha_alta + " " + df.hora_alta, format=FORMATO_ALTA[anio],
                                      errors="coerce").astype("datetime64[ns]")
    df["anio_fuente"] = pd.Series(anio, index=df.index, dtype="Int64")
    return df[list(ESQUEMA_USUARIOS)]


def calidad_usuarios(df: pd.DataFrame):
    """ID nulo -> cuarentena. Ids repetidos: queda el de fecha_alta más reciente, el resto a cuarentena."""
    df = df.sort_values(["fecha_alta", "anio_fuente"], ascending=False, na_position="last", kind="stable")
    motivo = pd.Series(pd.NA, index=df.index, dtype="string")
    motivo = motivo.mask(df.id_usuario.isna(), "ID_NULO")
    motivo = motivo.mask(motivo.isna() & df.id_usuario.duplicated(keep="first"), "DUPLICADO")
    validos = df[motivo.isna()].sort_values(["anio_fuente", "id_usuario"]).reset_index(drop=True)
    return validos, df[motivo.notna()].assign(motivo=motivo[motivo.notna()])


# ---------------------------------------------------------------- orquestación
def ejecutar() -> None:
    mapeo_est = reconciliar_estaciones_2016()
    mapeo_est.to_csv(SILVER / "mapeo_estaciones_2016.csv", index=False)
    print("  estaciones 2016 reconciliadas:", mapeo_est.criterio.value_counts().to_dict())
    mapa_2016 = dict(zip(mapeo_est.id_2016, mapeo_est.id_estacion))

    filas_bronze = {}
    partes = []
    for anio in ANIOS:
        df = preparar_viajes(anio, mapa_2016)
        filas_bronze[anio] = len(df)
        partes.append(df)
    viajes, cuarentena = aplicar_calidad(pd.concat(partes, ignore_index=True))
    del partes

    # Verificaciones antes de guardar: si algo falla, no se escribe nada.
    for anio in ANIOS:
        s, q = (viajes.anio_fuente == anio).sum(), (cuarentena.anio_fuente == anio).sum()
        assert filas_bronze[anio] == s + q, f"Conciliación {anio}: {filas_bronze[anio]} != {s} + {q}"
        print(f"  silver/viajes {anio}: {s:,} válidos + {q:,} en cuarentena = {filas_bronze[anio]:,} (bronze)")
    assert viajes.id_viaje.is_unique, "id_viaje repetido en silver/viajes"
    validar_esquema(viajes, ESQUEMA_VIAJES)
    validar_esquema(cuarentena.drop(columns="motivo"), ESQUEMA_VIAJES)

    usuarios_crudos = pd.concat([preparar_usuarios(a) for a in ANIOS], ignore_index=True)
    usuarios, usuarios_q = calidad_usuarios(usuarios_crudos)
    assert len(usuarios) + len(usuarios_q) == len(usuarios_crudos)
    assert usuarios.id_usuario.is_unique
    validar_esquema(usuarios, ESQUEMA_USUARIOS)
    print(f"  silver/usuarios: {len(usuarios):,} válidos + {len(usuarios_q):,} en cuarentena")

    escribir_parquet(viajes, SILVER / "viajes" / "data.parquet")
    escribir_parquet(cuarentena, SILVER / "viajes_cuarentena" / "data.parquet")
    escribir_parquet(usuarios, SILVER / "usuarios" / "data.parquet")
    escribir_parquet(usuarios_q, SILVER / "usuarios_cuarentena" / "data.parquet")
