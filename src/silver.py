"""Capa Silver: esquema canónico, tipos correctos, valores normalizados y cuarentena.

Esta es la capa donde está casi todo el trabajo. La organicé así:

  1. Esquemas canónicos  -> lo que tiene que salir, igual para los tres años.
  2. Mapeos por año      -> lo que cambia entre años. Lo concentré todo acá arriba, en diccionarios,
                            para que si mañana llega 2025 con otro formato solo haya que agregar una entrada.
  3. Helpers de limpieza -> funciones chicas que se reusan (ids, números, nombres).
  4. Reconciliación de estaciones de 2016 (el problema más difícil, DECISIONES.md D3).
  5. Viajes: preparar + reglas de calidad.   6. Usuarios: preparar + reglas de calidad.
  7. ejecutar(): orquesta todo y VERIFICA antes de escribir.

Cada vez que tomé una decisión que no estaba en el enunciado dejé la referencia "Dn" a DECISIONES.md.
"""
import unicodedata

import numpy as np
import pandas as pd

from src.bronze import escribir_parquet
from src.config import ANIOS, BRONZE, SILVER

# =============================================================================================
# 1. ESQUEMAS CANÓNICOS (los da el enunciado, Tarea 3, paso 1)
# =============================================================================================
# Los escribí como diccionario {columna: tipo} porque así me sirven para dos cosas a la vez:
# el orden de las columnas de salida (list(ESQUEMA)) y la validación final (validar_esquema).
# Uso "Int64" con mayúscula (entero que acepta nulos) y "string" (texto con nulos de verdad, pd.NA).
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

# =============================================================================================
# 2. MAPEOS POR AÑO (salen de la tabla de la Tarea 1, DECISIONES.md sección 1)
# =============================================================================================
# Las columnas de fechas y estaciones se llaman igual en los tres años, así que las puse una sola vez.
_COMUNES = {
    "fecha_origen_recorrido": "fecha_hora_inicio",
    "fecha_destino_recorrido": "fecha_hora_fin",
    "id_estacion_origen": "id_estacion_origen",
    "nombre_estacion_origen": "nombre_estacion_origen",
    "id_estacion_destino": "id_estacion_destino",
    "nombre_estacion_destino": "nombre_estacion_destino",
}
# Lo que NO está en el mapeo se descarta (coordenadas, direcciones, modelo de bici...: D8).
# 2016 no trae id de viaje (lo genero, D2), ni id de usuario (queda nulo), y su duración no la mapeo
# a propósito porque no coincide con las fechas: la calculo (D4).
# Ojo con 2019: el id del viaje es "Id_recorrido" con mayúscula.
MAPEO_VIAJES = {
    2016: {**_COMUNES},
    2019: {"Id_recorrido": "id_viaje", "duracion_recorrido": "duracion_seg",
           "id_usuario": "id_usuario", **_COMUNES},
    2024: {"id_recorrido": "id_viaje", "duracion_recorrido": "duracion_seg",
           "id_usuario": "id_usuario", **_COMUNES},
}
# Los tres años usan el mismo formato de fecha en viajes, pero lo dejo por año igual:
# si un año nuevo viene distinto, se cambia acá y nada más.
FORMATO_FECHA_VIAJES = {a: "%Y-%m-%d %H:%M:%S" for a in ANIOS}

# En usuarios solo cambia el nombre del id: en 2019 viene como "ID_usuario" (en mayúsculas).
MAPEO_USUARIOS = {
    2016: {"id_usuario": "id_usuario"},
    2019: {"ID_usuario": "id_usuario"},
    2024: {"id_usuario": "id_usuario"},
}
for _m in MAPEO_USUARIOS.values():
    _m.update(genero_usuario="genero", edad_usuario="edad", fecha_alta="fecha_alta", hora_alta="hora_alta")
# La fecha de alta viene partida en dos columnas (fecha + hora) y las junto antes de convertir.
# 2016 es el raro: día-mes-año con año de 2 dígitos y reloj de 12 horas ("01-01-16" + "3:45:19 AM").
FORMATO_ALTA = {2016: "%d-%m-%y %I:%M:%S %p", 2019: "%Y-%m-%d %H:%M:%S", 2024: "%Y-%m-%d %H:%M:%S"}

# Mapeo de género (Tarea 3, paso 4). Revisé con value_counts() qué aparece de verdad: solo
# M, F, MALE, FEMALE, OTHER y vacío. Agregué igual las variantes del enunciado (Femenino, X, etc.)
# para que el pipeline no se rompa si un año futuro las trae. Lo que no esté acá -> DESCONOCIDO.
MAPEO_GENERO = {
    "F": "F", "FEMALE": "F", "FEMENINO": "F", "MUJER": "F",
    "M": "M", "MALE": "M", "MASCULINO": "M", "HOMBRE": "M",
    "OTHER": "OTRO", "OTRO": "OTRO", "X": "OTRO", "NO BINARIO": "OTRO",
}

# Bronze guarda los campos vacíos como "" (D1) y en 2019 las edades faltantes vienen como el texto "NA".
# Acá decido que ambos significan "nulo" (D6).
VALORES_NULOS = ["", "NA"]
EDAD_MIN, EDAD_MAX = 16, 90  # fuera de este rango la edad pasa a nulo, no se rechaza al usuario (D10)
# Estos "nombres" aparecen en 2019 pero son placeholders del sistema, no nombres de estación.
# Si los dejaba, la estación de Puerto Madero UCA salía en el top 10 como "KP1808000368" (D5).
NOMBRES_PLACEHOLDER = {"KP1808000368", "ERRONEA", "PLANEADA", "OFF BRAUDACCO", "TEST TECNICO PDM"}
# Distancia máxima para considerar que una estación de 2016 y una de 2019/2024 son la misma (D3).
# Primero probé con 50 m y quedaban afuera OBELISCO, CONGRESO y ONCE, porque las coordenadas de 2016
# están corridas. Con 300 m (unas 3 cuadras) entran todas las obvias y no se cuela ningún error:
# el primer falso candidato está a 515 m (ver docs/img/d3_distancias.png).
DIST_MAX_RECONCILIACION_M = 300


# =============================================================================================
# 3. HELPERS DE LIMPIEZA
# =============================================================================================
def renombrar(df: pd.DataFrame, mapeo: dict) -> pd.DataFrame:
    """Renombra según el mapeo del año y se queda solo con esas columnas (la sugerencia del enunciado)."""
    return df.rename(columns=mapeo)[list(mapeo.values())]


def a_entero(s: pd.Series) -> pd.Series:
    """'5BAEcobici' -> 5, '992557.0' -> 992557, '2,814' -> 2814, '' -> <NA>.

    Esta función resuelve tres problemas de formato que encontré en la exploración:
      - 2019 le pega el sufijo "BAEcobici" a los ids de estación.
      - 2019 escribe las duraciones con coma de miles: "2,814" son 2814 segundos, NO 2,8.
        Lo comprobé: así coincide al 100 % con fin - inicio.
      - 2024 escribe algunos ids como float ("992557.0"); to_numeric lo resuelve solo.
    Con errors="coerce", si algo no se puede convertir queda nulo en vez de cortar el programa,
    y después la regla de calidad correspondiente lo manda a cuarentena.
    """
    s = s.str.replace("BAEcobici", "", regex=False).str.replace(",", "", regex=False)
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def limpiar_id(s: pd.Series) -> pd.Series:
    """Ids como texto limpio: '6243BAEcobici' -> '6243', '992557.0' -> '992557'.

    Los ids de viaje y de usuario quedan como texto (así lo pide el esquema), pero tienen que quedar
    limpios para que el mismo usuario se escriba igual en la tabla de viajes y en la de usuarios.
    Si no, el join de gold no encontraría a nadie.
    """
    return s.str.replace("BAEcobici", "", regex=False).str.replace(r"\.0$", "", regex=True).astype("string")


def normalizar_nombre(s: pd.Series) -> pd.Series:
    """'005 - Plaza Italia ' -> 'PLAZA ITALIA'; 'Pacífico' -> 'PACIFICO' (D5).

    El enunciado pide strip, mayúsculas y colapsar espacios. Yo agregué dos pasos más, porque sin ellos
    el mismo nombre de 2016 y de 2024 nunca quedaba igual:
      - sacar el prefijo numérico que traen 2019 y 2024 ("005 - ", "131- ", "048 -");
      - sacar las tildes: 2016 dice "Pacífico" y 2024 "Pacifico".
    """
    s = (s.str.replace(r"^\s*\d+\s*-\s*", "", regex=True)   # prefijo numérico ("005 - ")
          .str.strip().str.upper().str.replace(r"\s+", " ", regex=True))
    # Para las tildes uso unicodedata (NFKD separa la letra del acento y después descarto el acento).
    # Hay unas 500 estaciones distintas y 10 millones de filas: en vez de procesar fila por fila,
    # armo un diccionario con los nombres únicos y lo aplico con map(). Es muchísimo más rápido.
    sin_tildes = {v: unicodedata.normalize("NFKD", v).encode("ascii", "ignore").decode()
                  for v in s.dropna().unique()}
    s = s.map(sin_tildes).astype("string")
    return s.mask(s.isin(NOMBRES_PLACEHOLDER))  # placeholders -> nulo


def validar_esquema(df: pd.DataFrame, esquema: dict) -> None:
    """Compara columnas, orden y tipos contra el esquema canónico (lo sugiere el enunciado).
    La llamo siempre antes de guardar: si algo no coincide, prefiero que el pipeline se corte."""
    real = {c: str(t) for c, t in df.dtypes.items()}
    if real != esquema:
        raise TypeError(f"Esquema inválido.\nEsperado: {esquema}\nReal:     {real}")


# =============================================================================================
# 4. RECONCILIACIÓN DE ESTACIONES DE 2016 (D3)
# =============================================================================================
# El problema: en 2016 el id de estación es el NÚMERO PÚBLICO (el "059" de "059 - Coronel Diaz"),
# pero desde 2019 el id es INTERNO y es otro número. Si uso los ids tal cual, el id 59 es "Coronel
# Díaz" en 2016 e "Independencia" en 2019/2024, y gold mezclaría dos estaciones distintas.
# La solución: traducir cada id de 2016 al id moderno de la misma estación física, usando el nombre y
# la ubicación. Se hace en silver porque es limpieza: gold no tiene que enterarse de este problema.

def _distancia_m(lat1, lon1, lat2, lon2):
    """Distancia en metros entre coordenadas (fórmula de haversine).
    Está vectorizada con numpy: calcula todas contra todas (99 x ~500) de una vez."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6_371_000 * np.arcsin(np.sqrt(a))


def _catalogo_estaciones(anio: int) -> pd.DataFrame:
    """id, nombre normalizado y coordenadas de cada estación de un año, leídos de bronze.

    Uso solo las coordenadas de ORIGEN, porque en 2019 las de destino vienen corridas una columna:
    "long_estacion_destino" tiene la latitud y "lat_estacion_destino" trae "lat,long" juntas.
    Las coordenadas no están en el esquema de silver, por eso las leo directo de bronze.
    """
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
    """Devuelve la tabla id_2016 -> id_estacion, con el criterio que se usó en cada caso.

    Para cada estación de 2016 pruebo, en orden, del criterio más fuerte al más débil:
      1. MISMO_NOMBRE_Y_UBICACION: hay una estación moderna con el mismo nombre a menos de 300 m.
         -> uso su id. Ej.: CORONEL DIAZ 2016 (id 59) -> id 216.
         No alcanza solo con la distancia: ONCE (2016) está a 17 m de ONCE II, que es otra estación.
      2. MISMO_ID_Y_UBICACION: el mismo id existe después, en el mismo lugar, pero con otro nombre.
         -> es una estación renombrada, mantengo el id. Ej.: ZOOLOGICO -> ECOPARQUE (id 44).
      3. ID_SIN_USO_POSTERIOR: el id no se volvió a usar -> lo mantengo, no choca con nadie.
      4. ESTACION_SOLO_2016: el id se reusó después para OTRA estación lejana.
         -> le doy un id nuevo, 1000 + id (1048 = "la 48 de 2016"), para que no se mezclen.
    """
    viejas = _catalogo_estaciones(2016)
    # Para las modernas priorizo 2024 (drop_duplicates se queda con la primera aparición).
    nuevas = pd.concat([_catalogo_estaciones(2024), _catalogo_estaciones(2019)]).drop_duplicates("id")
    dist = _distancia_m(viejas.lat.to_numpy()[:, None], viejas.lon.to_numpy()[:, None],
                        nuevas.lat.to_numpy()[None, :], nuevas.lon.to_numpy()[None, :])
    por_id = nuevas.set_index("id")
    # El 1000 no está escrito a mano: es la potencia de 10 siguiente al id moderno más grande (5xx).
    # Si un año nuevo trajera ids de 4 cifras, el offset pasaría solo a 10000.
    offset = 10 ** len(str(nuevas.id.max()))
    filas = []
    for i, v in enumerate(viejas.itertuples()):
        mismo_nombre = (nuevas.nombre.to_numpy() == v.nombre) & (dist[i] < DIST_MAX_RECONCILIACION_M)
        mismo_id_cerca = v.id in por_id.index and dist[i][nuevas.id.to_numpy() == v.id][0] < DIST_MAX_RECONCILIACION_M
        if mismo_nombre.any():
            # si hubiera más de una con el mismo nombre cerca, me quedo con la más cercana
            nuevo, criterio = int(nuevas.id.to_numpy()[mismo_nombre][dist[i][mismo_nombre].argmin()]), "MISMO_NOMBRE_Y_UBICACION"
        elif mismo_id_cerca:
            nuevo, criterio = v.id, "MISMO_ID_Y_UBICACION"
        elif v.id not in por_id.index:
            nuevo, criterio = v.id, "ID_SIN_USO_POSTERIOR"
        else:
            nuevo, criterio = offset + v.id, "ESTACION_SOLO_2016"
        filas.append((v.id, v.nombre, nuevo, por_id.nombre.get(nuevo, v.nombre), criterio))
    return pd.DataFrame(filas, columns=["id_2016", "nombre_2016", "id_estacion", "nombre_actual", "criterio"])


# =============================================================================================
# 5. VIAJES
# =============================================================================================
def preparar_viajes(anio: int, mapa_2016: dict) -> pd.DataFrame:
    """Lleva un año de bronze al esquema canónico: renombra, tipa, normaliza y completa columnas.
    Todavía NO filtra nada: las reglas de calidad se aplican después, sobre los tres años juntos."""
    mapeo = MAPEO_VIAJES[anio]
    # Parquet es columnar: le pido a bronze solo las columnas que voy a usar y no las 15 a 18 del archivo.
    # Con 6 millones de filas en 2019, esto ahorra mucha memoria.
    df = pd.read_parquet(BRONZE / "recorridos" / f"anio={anio}" / "data.parquet", columns=list(mapeo))
    df = renombrar(df, mapeo).replace(VALORES_NULOS, pd.NA)

    # Fechas: errors="coerce" convierte lo inválido en NaT y la regla 1 (FECHA_INVALIDA) lo captura.
    # El .astype("datetime64[ns]") lo agregué porque pandas 3 devuelve microsegundos por defecto
    # y el esquema pide nanosegundos.
    fmt = FORMATO_FECHA_VIAJES[anio]
    for c in ("fecha_hora_inicio", "fecha_hora_fin"):
        df[c] = pd.to_datetime(df[c], format=fmt, errors="coerce").astype("datetime64[ns]")
    for c in ("id_estacion_origen", "id_estacion_destino"):
        df[c] = a_entero(df[c])
        if anio == 2016:  # traduzco los ids de 2016 a los modernos (D3)
            df[c] = df[c].map(mapa_2016).astype("Int64")
    for c in ("nombre_estacion_origen", "nombre_estacion_destino"):
        df[c] = normalizar_nombre(df[c])

    if "duracion_seg" in df:
        # 2019 y 2024 la traen en segundos y coincide con fin - inicio: uso la de la fuente.
        df["duracion_seg"] = a_entero(df["duracion_seg"])
    else:
        # 2016: el campo original ("0 days 00:16:29") no tiene relación con las fechas del mismo viaje
        # (correlación 0,002, ver docs/img/d4_duracion_2016.png). El enunciado dice que si la duración
        # viene en otra unidad se calcule de las fechas, y así además queda coherente con ellas (D4).
        seg = (df.fecha_hora_fin - df.fecha_hora_inicio).dt.total_seconds()
        df["duracion_seg"] = seg.round().astype("Int64")

    if "id_viaje" in df:
        df["id_viaje"] = limpiar_id(df["id_viaje"])
    else:
        # 2016 no trae id. Lo genero con el número de fila: "2016-1", "2016-2"... (D2)
        # Es determinístico (el archivo siempre tiene el mismo orden), así que el pipeline sigue
        # siendo idempotente, y el prefijo "2016-" asegura que no choque con los ids de otros años.
        df["id_viaje"] = ("2016-" + pd.Series(np.arange(1, len(df) + 1), index=df.index).astype(str)).astype("string")
    # 2016 tampoco trae usuario: la columna existe igual (el esquema es uno solo), pero vacía.
    df["id_usuario"] = limpiar_id(df["id_usuario"]) if "id_usuario" in df else pd.Series(pd.NA, index=df.index, dtype="string")
    df["anio_fuente"] = pd.Series(anio, index=df.index, dtype="Int64")
    return df[list(ESQUEMA_VIAJES)]  # mismo orden de columnas que el esquema canónico


def aplicar_calidad(df: pd.DataFrame):
    """Devuelve (validos, cuarentena). Las 5 reglas del enunciado, en el orden del enunciado.

    Ninguna fila se borra: la que falla va a cuarentena con el PRIMER motivo que falló.
    El truco para quedarme con el primero es "motivo.isna() & condicion": una fila que ya tiene
    motivo no se pisa con las reglas siguientes.
    """
    motivo = pd.Series(pd.NA, index=df.index, dtype="string")
    reglas = [
        ("FECHA_INVALIDA", df.fecha_hora_inicio.isna() | df.fecha_hora_fin.isna()),
        ("ESTACION_NULA", df.id_estacion_origen.isna() | df.id_estacion_destino.isna()),
        ("FIN_ANTERIOR_A_INICIO", df.fecha_hora_fin < df.fecha_hora_inicio),
        ("DURACION_MAYOR_24H", df.duracion_seg > 86_400),
        # keep="first": la primera aparición es válida y las repeticiones van a cuarentena (D7).
        ("DUPLICADO", df.id_viaje.duplicated(keep="first")),
    ]
    for nombre, condicion in reglas:
        # fillna(False): si la comparación da nulo (por ejemplo, una fecha NaT) no cuenta como falla
        # de ESTA regla; esa fila ya la agarró la regla que corresponde.
        motivo = motivo.mask(motivo.isna() & condicion.fillna(False), nombre)
    validos = df[motivo.isna()]
    cuarentena = df[motivo.notna()].assign(motivo=motivo[motivo.notna()])
    return validos, cuarentena


# =============================================================================================
# 6. USUARIOS
# =============================================================================================
def preparar_usuarios(anio: int) -> pd.DataFrame:
    """Mismo esquema de pasos que en viajes: renombrar, nulos, tipos y normalización."""
    mapeo = MAPEO_USUARIOS[anio]
    df = pd.read_parquet(BRONZE / "usuarios" / f"anio={anio}" / "data.parquet", columns=list(mapeo))
    df = renombrar(df, mapeo).replace(VALORES_NULOS, pd.NA)
    df["id_usuario"] = limpiar_id(df["id_usuario"])
    # strip + upper antes del mapeo: así "Female", " female " y "FEMALE" caen todos en F.
    df["genero"] = df["genero"].str.strip().str.upper().map(MAPEO_GENERO).fillna("DESCONOCIDO").astype("string")
    edad = pd.to_numeric(df["edad"], errors="coerce").astype("Int64")
    # Edades fuera de [16, 90] (aparecen 0, 4, 935, 2021...) -> nulo. No rechazo al usuario,
    # porque su género y su fecha de alta siguen siendo datos válidos (D10).
    df["edad"] = edad.where(edad.between(EDAD_MIN, EDAD_MAX))
    df["fecha_alta"] = pd.to_datetime(df.fecha_alta + " " + df.hora_alta, format=FORMATO_ALTA[anio],
                                      errors="coerce").astype("datetime64[ns]")
    df["anio_fuente"] = pd.Series(anio, index=df.index, dtype="Int64")
    return df[list(ESQUEMA_USUARIOS)]


def calidad_usuarios(df: pd.DataFrame):
    """Reglas del enunciado para usuarios: id no nulo y sin duplicados.

    Si un id aparece más de una vez me quedo con el de fecha_alta más reciente. Para eso ordeno de más
    nuevo a más viejo y marco como DUPLICADO todo lo que no sea la primera aparición.
    Pasa con 5.231 ids que están en 2016 y en 2019. Revisé esos casos y casi seguro son personas
    distintas (el género coincide solo en el 39 %), pero no hay forma de separarlas con estos datos,
    así que aplico la regla y lo dejo documentado (D11).
    Los rechazados no se pierden: van a usuarios_cuarentena, igual que en viajes (D12).
    """
    df = df.sort_values(["fecha_alta", "anio_fuente"], ascending=False, na_position="last", kind="stable")
    motivo = pd.Series(pd.NA, index=df.index, dtype="string")
    motivo = motivo.mask(df.id_usuario.isna(), "ID_NULO")
    motivo = motivo.mask(motivo.isna() & df.id_usuario.duplicated(keep="first"), "DUPLICADO")
    # Vuelvo a ordenar los válidos por año e id, para que la salida sea estable entre corridas.
    validos = df[motivo.isna()].sort_values(["anio_fuente", "id_usuario"]).reset_index(drop=True)
    return validos, df[motivo.notna()].assign(motivo=motivo[motivo.notna()])


# =============================================================================================
# 7. ORQUESTACIÓN
# =============================================================================================
def ejecutar() -> None:
    # Primero armo la tabla de equivalencias de estaciones y la guardo en CSV, así se puede auditar
    # a mano qué estación de 2016 terminó en qué id y con qué criterio.
    mapeo_est = reconciliar_estaciones_2016()
    mapeo_est.to_csv(SILVER / "mapeo_estaciones_2016.csv", index=False)
    print("  estaciones 2016 reconciliadas:", mapeo_est.criterio.value_counts().to_dict())
    mapa_2016 = dict(zip(mapeo_est.id_2016, mapeo_est.id_estacion))

    filas_bronze = {}
    partes = []
    for anio in ANIOS:
        df = preparar_viajes(anio, mapa_2016)
        filas_bronze[anio] = len(df)  # lo guardo para la conciliación de abajo
        partes.append(df)
    # Aplico la calidad a los tres años juntos (no año por año) para que la regla DUPLICADO
    # también detecte un id repetido entre años distintos.
    viajes, cuarentena = aplicar_calidad(pd.concat(partes, ignore_index=True))
    del partes  # libero memoria: son ~10 millones de filas

    # Verificaciones del paso 6 del enunciado. Las hago ANTES de escribir: si algo falla,
    # el pipeline se corta y no deja en disco una capa silver inconsistente.
    for anio in ANIOS:
        s, q = (viajes.anio_fuente == anio).sum(), (cuarentena.anio_fuente == anio).sum()
        # Conciliación: no se puede perder ni un registro.
        assert filas_bronze[anio] == s + q, f"Conciliación {anio}: {filas_bronze[anio]} != {s} + {q}"
        print(f"  silver/viajes {anio}: {s:,} válidos + {q:,} en cuarentena = {filas_bronze[anio]:,} (bronze)")
    assert viajes.id_viaje.is_unique, "id_viaje repetido en silver/viajes"
    validar_esquema(viajes, ESQUEMA_VIAJES)
    validar_esquema(cuarentena.drop(columns="motivo"), ESQUEMA_VIAJES)

    usuarios_crudos = pd.concat([preparar_usuarios(a) for a in ANIOS], ignore_index=True)
    usuarios, usuarios_q = calidad_usuarios(usuarios_crudos)
    assert len(usuarios) + len(usuarios_q) == len(usuarios_crudos)  # conciliación de usuarios
    assert usuarios.id_usuario.is_unique
    validar_esquema(usuarios, ESQUEMA_USUARIOS)
    print(f"  silver/usuarios: {len(usuarios):,} válidos + {len(usuarios_q):,} en cuarentena")

    escribir_parquet(viajes, SILVER / "viajes" / "data.parquet")
    escribir_parquet(cuarentena, SILVER / "viajes_cuarentena" / "data.parquet")
    escribir_parquet(usuarios, SILVER / "usuarios" / "data.parquet")
    escribir_parquet(usuarios_q, SILVER / "usuarios_cuarentena" / "data.parquet")
