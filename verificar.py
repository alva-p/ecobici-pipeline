"""Verificaciones del pipeline (sin frameworks: solo asserts).

    python verificar.py                 # reglas unitarias + salidas ya generadas
    python verificar.py --idempotencia  # además corre el pipeline 2 veces y compara

Correr después de `python main.py`.
"""
import sys

import pandas as pd

from src import gold, silver
from src.config import ANIOS, BRONZE, GOLD, REPORTES, SILVER

OK = []


def check(nombre, condicion, detalle=""):
    assert condicion, f"FALLA: {nombre} {detalle}"
    OK.append(nombre)
    print(f"  ok  {nombre}")


def reglas_unitarias():
    print("[Reglas de transformación]")
    s = pd.Series(["5BAEcobici", "992557.0", "2,814", "", None], dtype="string")
    check("a_entero limpia sufijos, '.0' y coma de miles",
          silver.a_entero(s.replace("", pd.NA)).tolist()[:3] == [5, 992557, 2814])
    check("limpiar_id", silver.limpiar_id(pd.Series(["6243BAEcobici", "992557.0"])).tolist() == ["6243", "992557"])
    n = silver.normalizar_nombre(pd.Series([" Pacífico ", "014 - Pacifico", "PACIFICO", "019 - PLAZA  SAN MARTIN", "KP1808000368"]))
    check("normalizar_nombre unifica variantes", n.iloc[:3].nunique() == 1 and n.iloc[0] == "PACIFICO")
    check("normalizar_nombre colapsa espacios", n.iloc[3] == "PLAZA SAN MARTIN")
    check("nombres placeholder -> nulo", pd.isna(n.iloc[4]))
    g = pd.Series(["FEMALE", "Male", "f", "OTHER", "X", "", "raro"]).str.upper().map(silver.MAPEO_GENERO).fillna("DESCONOCIDO")
    check("mapeo de género", g.tolist() == ["F", "M", "F", "OTRO", "OTRO", "DESCONOCIDO", "DESCONOCIDO"])

    t = pd.Timestamp
    df = pd.DataFrame({
        "id_viaje": pd.array(["1", "2", "3", "4", "4", "5"], dtype="string"),
        "fecha_hora_inicio": [pd.NaT, t("2019-01-01 10:00"), t("2019-01-01 10:00"), t("2019-01-01"), t("2019-01-01"), t("2019-01-01")],
        "fecha_hora_fin": [t("2019-01-01"), t("2019-01-01 09:00"), t("2019-01-03"), t("2019-01-01 01:00"), t("2019-01-01 01:00"), t("2019-01-01 01:00")],
        "duracion_seg": pd.array([10, -3600, 172800, 3600, 3600, 3600], dtype="Int64"),
        "id_estacion_origen": pd.array([None, 1, 1, 1, 1, 1], dtype="Int64"),  # fila 1 falla 2 reglas
        "id_estacion_destino": pd.array([1, 1, 1, 1, 1, None], dtype="Int64"),
    })
    validos, q = silver.aplicar_calidad(df)
    check("cuarentena guarda solo el primer motivo y respeta el orden",
          q.motivo.tolist() == ["FECHA_INVALIDA", "FIN_ANTERIOR_A_INICIO", "DURACION_MAYOR_24H", "DUPLICADO", "ESTACION_NULA"])
    check("válidos + cuarentena = entrada", len(validos) + len(q) == len(df) and validos.id_viaje.tolist() == ["4"])

    r = gold.rango_etario(pd.Series([16, 25, 26, 35, 36, 50, 51, None], dtype="Int64"))
    check("rangos etarios en los bordes", r.tolist() == ["16-25", "16-25", "26-35", "26-35", "36-50", "36-50", "51+", "DESCONOCIDO"])


def salidas():
    print("[Salidas del pipeline]")
    v = pd.read_parquet(SILVER / "viajes" / "data.parquet")
    q = pd.read_parquet(SILVER / "viajes_cuarentena" / "data.parquet", columns=["anio_fuente", "motivo"])
    for a in ANIOS:
        b = pd.read_parquet(BRONZE / "recorridos" / f"anio={a}" / "data.parquet", columns=["_anio"]).shape[0]
        check(f"conciliación {a}: bronze = silver + cuarentena", b == (v.anio_fuente == a).sum() + (q.anio_fuente == a).sum())
    check("id_viaje único en silver", v.id_viaje.is_unique)
    silver.validar_esquema(v, silver.ESQUEMA_VIAJES)
    check("esquema canónico silver/viajes", True)
    silver.validar_esquema(pd.read_parquet(SILVER / "usuarios" / "data.parquet"), silver.ESQUEMA_USUARIOS)
    check("esquema canónico silver/usuarios", True)
    check("motivos válidos", set(q.motivo) <= {"FECHA_INVALIDA", "ESTACION_NULA", "FIN_ANTERIOR_A_INICIO", "DURACION_MAYOR_24H", "DUPLICADO"})

    f = pd.read_parquet(GOLD / "fact_viajes" / "data.parquet")
    df_ = pd.read_parquet(GOLD / "dim_fecha" / "data.parquet")
    de = pd.read_parquet(GOLD / "dim_estacion" / "data.parquet")
    check("fact_viajes tiene las mismas filas que silver/viajes", len(f) == len(v))
    check("toda fecha_key existe en dim_fecha", f.fecha_key.isin(df_.fecha_key).all())
    check("toda estación existe en dim_estacion", f.id_estacion_origen.isin(de.id_estacion).all() and f.id_estacion_destino.isin(de.id_estacion).all())
    check("dim_fecha sin huecos", df_.fecha.diff().dropna().eq(pd.Timedelta(days=1)).all())
    for r in ("viajes_por_mes", "top10_estaciones_por_anio", "duracion_por_rango_y_genero", "viajes_circulares", "calidad_por_anio"):
        check(f"reporte {r}.csv existe y no está vacío", len(pd.read_csv(REPORTES / f"{r}.csv")) > 0)
    tv = pd.read_csv(REPORTES / "viajes_por_mes.csv").cantidad_viajes.sum()
    check("reporte 1 suma todos los viajes de fact", tv == len(f))


def huella() -> dict:
    """Hash del contenido de cada tabla, sin _fecha_ingesta (que cambia en cada corrida a propósito)."""
    h = {}
    for p in sorted(list(BRONZE.rglob("*.parquet")) + list(SILVER.rglob("*.parquet")) + list(GOLD.rglob("*.parquet"))):
        df = pd.read_parquet(p)
        h[str(p)] = (len(df), int(pd.util.hash_pandas_object(df.drop(columns="_fecha_ingesta", errors="ignore"), index=False).sum()))
    for p in sorted(REPORTES.glob("*.csv")):
        h[str(p)] = p.read_bytes()
    return h


def idempotencia():
    import main
    print("[Idempotencia: se corre el pipeline 2 veces]")
    main.main()
    antes = huella()
    main.main()
    despues = huella()
    check("mismas tablas y mismo contenido tras 2 ejecuciones", antes == despues,
          [k for k in antes if antes.get(k) != despues.get(k)])


if __name__ == "__main__":
    reglas_unitarias()
    salidas()
    if "--idempotencia" in sys.argv:
        idempotencia()
    print(f"\n{len(OK)} verificaciones OK")
