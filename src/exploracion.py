"""Tarea 1: exploración de los archivos crudos, antes de escribir ninguna capa.

Uso:  python -m src.exploracion

Como pide el enunciado, leo solo las primeras 1000 filas de cada archivo. Uso el mismo lector que
bronze (todo como texto), así veo los valores tal cual vienen y no lo que pandas adivina.
Lo que salió de acá está resumido en la tabla de la sección 1 de DECISIONES.md.
No forma parte de main.py: es la mirada previa que usé para decidir los mapeos de silver.
"""
from src.bronze import ARCHIVOS, leer_crudo
from src.config import RAW


def explorar(tabla: str, anio: int, archivo: str) -> None:
    df = leer_crudo(RAW / archivo, nrows=1000)
    print(f"\n=== {tabla} {anio}: {archivo} ({df.shape[1]} columnas)")
    # Una fila de ejemplo por columna, más cuántos vacíos hay en la muestra. Con esto se ven de un
    # vistazo los formatos de fecha, los sufijos "BAEcobici", la coma de miles y los nombres con prefijo.
    resumen = df.head(1).T.rename(columns={0: "ejemplo"})
    resumen["vacios_en_1000"] = (df == "").sum()
    resumen["valores_distintos"] = df.nunique()
    print(resumen.to_string())
    for col in ("genero", "genero_usuario", "género"):  # el nombre de la columna de género cambia por año
        if col in df:
            print(f"  valores de {col}: {sorted(df[col].unique())}")


def main() -> None:
    for tabla, por_anio in ARCHIVOS.items():
        for anio, archivo in por_anio.items():
            explorar(tabla, anio, archivo)


if __name__ == "__main__":
    main()
