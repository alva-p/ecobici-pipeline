# Decisiones del pipeline

Este documento reúne lo que encontré en los archivos (Tarea 1), los mapeos que usa silver y cada decisión que
no estaba indicada en el enunciado, con su justificación. Los números salen de la última ejecución del pipeline
sobre los datos completos.

---

## 1. Exploración de los archivos (Tarea 1)

### Recorridos

| Aspecto | 2016 | 2019 | 2024 |
|---|---|---|---|
| Archivo dentro del ZIP | `recorridos-realizados-2016.csv` | `trips_2019.csv` | `badata_ecobici_recorridos_realizados_2024.csv` |
| Separador | `,` | `,` (todos los textos entre comillas) | `,` |
| Codificación | UTF-8 | UTF-8 | UTF-8 **con BOM** |
| Columnas | 15: `periodo, genero_usuario, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, long_estacion_origen, lat_estacion_origen, domicilio_estacion_origen, duracion_recorrido, fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, long_estacion_destino, lat_estacion_destino, domicilio_estacion_destino` | 18: *(primera columna sin nombre, pandas la llama `Unnamed: 0`)*, `Id_recorrido, duracion_recorrido, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, direccion_estacion_origen, long_estacion_origen, lat_estacion_origen, fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, direccion_estacion_destino, long_estacion_destino, lat_estacion_destino, id_usuario, modelo_bicicleta, género` | 17: `id_recorrido, duracion_recorrido, fecha_origen_recorrido, id_estacion_origen, nombre_estacion_origen, direccion_estacion_origen, long_estacion_origen, lat_estacion_origen, fecha_destino_recorrido, id_estacion_destino, nombre_estacion_destino, direccion_estacion_destino, long_estacion_destino, lat_estacion_destino, id_usuario, modelo_bicicleta, genero` |
| Formato de fecha y hora | `2016-01-01 02:21:41` (`%Y-%m-%d %H:%M:%S`) | igual | igual |
| ¿Trae duración? ¿Unidad? | Sí, como texto timedelta: `0 days 00:16:29.000000000`. **No coincide con fin − inicio** (ver D4) | Sí, en segundos, **con coma de miles**: `2,814` = 2814 s. Coincide al 100 % con fin − inicio | Sí, en segundos enteros. Coincide en el 99,9 % de los casos |
| ¿Trae id de usuario? | **No** | Sí, con sufijo: `6243BAEcobici` | Sí, como float: `992557.0` |
| Id de viaje | **No trae** | `9379BAEcobici` | `20428222` |
| Id de estación | número público de la estación (ver D3) | `5BAEcobici` (id interno) | `513` (id interno) |
| Nombre de estación | `Pacífico` (con tildes, sin prefijo) | `005 - Plaza Italia` | `308 - SAN MARTIN II` |
| Cantidad total de filas | **596.807** | **6.217.004** | **3.559.284** |
| Otras particularidades | 93.555 viajes sin fecha de destino (15,7 %). Trae género en el viaje | Coordenadas de destino corridas: `long_estacion_destino` tiene la latitud y `lat_estacion_destino` tiene "lat,long". 1.760 viajes sin estación de destino. El año arranca el 9 de febrero | 3.379 viajes sin fecha de destino. 1 fila duplicada exacta |

### Usuarios

| Aspecto | 2016 | 2019 | 2024 |
|---|---|---|---|
| Archivo | `usuarios-ecobici-2016.csv` | `usuarios_ecobici_2019.csv` | `usuarios_ecobici_2024.csv` |
| Separador | `,` | `,` (textos entre comillas) | `,` |
| Codificación | UTF-8 | UTF-8 | UTF-8 **con BOM** |
| Columnas | `id_usuario, genero_usuario, edad_usuario, fecha_alta, hora_alta` | `ID_usuario` (**en mayúsculas**), `genero_usuario, edad_usuario, fecha_alta, hora_alta` | `id_usuario, genero_usuario, edad_usuario, fecha_alta, hora_alta` |
| Formato de fecha y hora | `01-01-16` + `3:45:19 AM` (**día-mes-año de 2 dígitos y reloj de 12 h**) | `2019-12-31` + `10:46:10` | `2024-01-18` + `09:21:02` |
| Valores de género | `M`, `F` | `MALE`, `FEMALE`, `OTHER`, vacío (857) | `MALE`, `FEMALE`, `OTHER` |
| Edad | 16 a 88 | texto `"NA"` en 765 filas; rango 0 a 935 | rango 4 a 2021 |
| Cantidad total de filas | **39.481** | **83.600** | **197.077** |

---

## 2. Mapeo de columnas (silver)

Está en `src/silver.py` (`MAPEO_VIAJES`, `MAPEO_USUARIOS`). Lo que no figura en el mapeo se descarta o se calcula.

### Viajes

| Canónica | 2016 | 2019 | 2024 |
|---|---|---|---|
| `id_viaje` | *(se genera, D2)* | `Id_recorrido` | `id_recorrido` |
| `fecha_hora_inicio` | `fecha_origen_recorrido` | `fecha_origen_recorrido` | `fecha_origen_recorrido` |
| `fecha_hora_fin` | `fecha_destino_recorrido` | `fecha_destino_recorrido` | `fecha_destino_recorrido` |
| `duracion_seg` | *(se calcula fin − inicio, D4)* | `duracion_recorrido` | `duracion_recorrido` |
| `id_estacion_origen` | `id_estacion_origen` *(reconciliado, D3)* | `id_estacion_origen` | `id_estacion_origen` |
| `nombre_estacion_origen` | `nombre_estacion_origen` | `nombre_estacion_origen` | `nombre_estacion_origen` |
| `id_estacion_destino` | `id_estacion_destino` *(reconciliado, D3)* | `id_estacion_destino` | `id_estacion_destino` |
| `nombre_estacion_destino` | `nombre_estacion_destino` | `nombre_estacion_destino` | `nombre_estacion_destino` |
| `id_usuario` | *(no existe: nulo)* | `id_usuario` | `id_usuario` |
| `anio_fuente` | *(constante 2016)* | *(constante 2019)* | *(constante 2024)* |

### Usuarios

| Canónica | 2016 | 2019 | 2024 |
|---|---|---|---|
| `id_usuario` | `id_usuario` | `ID_usuario` | `id_usuario` |
| `genero` | `genero_usuario` | `genero_usuario` | `genero_usuario` |
| `edad` | `edad_usuario` | `edad_usuario` | `edad_usuario` |
| `fecha_alta` | `fecha_alta` + `hora_alta` (`%d-%m-%y %I:%M:%S %p`) | `fecha_alta` + `hora_alta` (`%Y-%m-%d %H:%M:%S`) | igual que 2019 |

### Valores de género

| Valor original | Silver |
|---|---|
| `F`, `FEMALE`, `FEMENINO`, `MUJER` | `F` |
| `M`, `MALE`, `MASCULINO`, `HOMBRE` | `M` |
| `OTHER`, `OTRO`, `X`, `NO BINARIO` | `OTRO` |
| vacío, nulo, cualquier otro | `DESCONOCIDO` |

La comparación se hace después de `strip()` y `upper()`, así que `Female` o ` female ` también caen en `F`.
En los datos reales solo aparecen `M`, `F`, `MALE`, `FEMALE`, `OTHER` y vacío; el resto del mapeo cubre las variantes que menciona el enunciado.

---

## 3. Decisiones que no están en el enunciado

### Bronze

**D1. Lectura sin interpretar nulos (`keep_default_na=False`).** Por defecto pandas convierte `""` y `"NA"` en NaN
aunque se use `dtype=str`. Con esta opción bronze guarda exactamente el texto que vino. Decidir qué es nulo es trabajo de silver (D6).

- Se lee con `encoding="utf-8-sig"`. El BOM es un artefacto de codificación, no un dato: sin esto, la primera columna de 2024 se llamaría `﻿id_recorrido`.
- La columna sin nombre de 2019 queda como `Unnamed: 0`, que es como pandas nombra un encabezado vacío. Es un índice de fila del sistema origen y silver lo descarta.
- El ZIP se abre con `zipfile` y se toma el único `.csv` que contiene. Si hubiera más de uno, el pipeline falla con un mensaje claro en lugar de adivinar.
- Escritura atómica: cada Parquet se escribe a un `.tmp` y después se renombra. Si el proceso se corta, nunca queda una tabla a medio escribir (es una de las cosas que aportaría Delta Lake o Iceberg, sección 3.6).

### Silver: viajes

**D2. `id_viaje` de 2016 sintético: `"2016-<n° de fila>"`.** El archivo no trae id. Se genera en el orden del archivo,
que es fijo, así que el id es determinístico y el pipeline sigue siendo idempotente. Antes verifiqué que 2016 no tiene filas duplicadas exactas, así que no se pierde ningún control de duplicados.
En 2019 se quita el sufijo `BAEcobici`. Verifiqué que los ids resultantes (10 a 6.875.895) no chocan con los de 2024 (20.180.969 a 24.503.283).

**D3. Reconciliación de ids de estación de 2016.** *Este fue el problema más importante del trabajo.* En 2016 el id de
estación es el **número público** (el mismo que aparece como prefijo en `059 - Coronel Diaz`). Desde 2019 el id es
**interno** y otro número. Si se usara el id tal cual, el id 59 sería "Coronel Díaz" en 2016 e "Independencia" en
2019/2024, y dim_estacion (que toma el nombre más reciente) le pondría a los viajes de 2016 el nombre de otra estación.

Solución en silver (`reconciliar_estaciones_2016()`): para cada estación de 2016, en este orden:

| Criterio | Regla | Estaciones |
|---|---|---|
| `MISMO_NOMBRE_Y_UBICACION` | existe en 2019/2024 una estación con el mismo nombre normalizado a menos de 300 m → se usa su id | 80 |
| `MISMO_ID_Y_UBICACION` | el mismo id existe después, a menos de 300 m, con otro nombre (estación renombrada) → se mantiene el id | 11 |
| `ID_SIN_USO_POSTERIOR` | el id no se usa después → se mantiene | 0 |
| `ESTACION_SOLO_2016` | el id se reusó después para otra estación lejana → id nuevo `1000 + id` | 8 |

- **Por qué 300 m:** las coordenadas de 2016 están corridas respecto de las de 2019/2024 (mediana 32 m; hasta unos 270 m en estaciones que claramente son la misma, por ejemplo LAVALLE). Con 50 m quedaban afuera OBELISCO, CONGRESO y ONCE. Con 300 m (unas tres cuadras) no se cuela ningún falso positivo: el siguiente candidato es MAIPU, a 515 m.
- **Por qué no solo por coordenadas:** la estación más cercana no alcanza. ONCE (2016) queda a 17 m de ONCE II, que es otra estación.
- **Por qué `1000 + id`:** es un rango que no usa ningún id real (el máximo es 5xx), y se lee fácil: 1048 es "la 48 de 2016".
- Solo se usan las coordenadas de **origen**, porque las de destino de 2019 vienen corridas.

Casos no triviales:

| id 2016 | Nombre 2016 | id silver | Nombre actual | Criterio |
|---|---|---|---|---|
| 18 | INDEPENDENCIA | 59 | INDEPENDENCIA | mismo nombre |
| 59 | CORONEL DIAZ | 216 | CORONEL DIAZ | mismo nombre |
| 1 | FACULTAD DE DERECHO | 202 | FACULTAD DE DERECHO | mismo nombre |
| 16 | LEGISLATURA | 441 | LEGISLATURA | mismo nombre |
| 19 | PLAZA SAN MARTIN | 536 | PLAZA SAN MARTIN | mismo nombre |
| 61 | MINISTERIO DE ECONOMIA | 444 | MINISTERIO DE ECONOMIA | mismo nombre |
| 97 | AVELINO DIAZ | 220 | AVELINO DIAZ | mismo nombre |
| 67 | SENILLOSA | 218 | SENILLOSA | mismo nombre |
| 44 | ZOOLOGICO | 44 | ECOPARQUE | mismo id (renombrada) |
| 2 | RETIRO | 2 | RETIRO I | mismo id |
| 26 | JUANA MANSO | 26 | JUANA MANSO I | mismo id |
| 49 | TREINTA Y TRES ORIENTALES | 49 | 33 ORIENTALES | mismo id |
| 114 | DELLA PAOLERA | 114 | TORRE MACRO | mismo id |
| 81 | BALCARCE | 81 | EX BALCARCE | mismo id |
| 47 | COLEGIO NACIONAL BUENOS AIRES | 47 | COLEGIO NACIONAL DE BUENOS AIRES | mismo id |
| 166 | CEMENTERIO DE LA RECOLETA | 166 | CEMENTERIO DE RECOLETA | mismo id |
| 41 | URQUIZA | 41 | PARQUE PATRICIOS II | mismo id |
| 10 | PUERTO MADERO - UCA | 10 | *(en 2019 figura como `KP1808000368`, ver D5)* | mismo id |
| 37 | PIEDRAS | 37 | *(en 2019 figura como `off braudacco`, ver D5)* | mismo id |
| 34, 36, 48, 68, 77, 80, 98, 150 | GALERIAS PACIFICO, MAIPU, PERON, RIVAROLA, SARANDI, DOBLAS, MORENO, VERA PENALOZA | 1034, 1036, … | (igual) | solo 2016 |

La tabla completa (99 filas) se genera en cada corrida en `data/silver/mapeo_estaciones_2016.csv`.

**D4. Duración de 2016 calculada como fin − inicio.** El campo original (`0 days 00:16:29`) **no tiene relación con
las fechas del mismo viaje**: la correlación entre ambos es 0,002, y los dos se distribuyen entre 10 y 180 minutos.
Los segundos del campo se reparten de forma perfectamente uniforme, lo que sugiere que los datos de 2016 fueron
anonimizados o perturbados por la fuente. El enunciado pide calcularla cuando viene "en otra unidad", y así
`duracion_seg` queda coherente con `fecha_hora_inicio` y `fecha_hora_fin`, que es lo que usan las reglas 3 y 4.
Para 2019 y 2024 se usa el campo de la fuente, que coincide con fin − inicio (100 % y 99,9 %).
Consecuencia: los 93.555 viajes de 2016 sin fecha de fin quedan sin duración y van a cuarentena por `FECHA_INVALIDA`.

**D5. Normalización de nombres de estación.** Además de lo que pide el enunciado (`strip`, mayúsculas, espacios
múltiples), se agregaron dos pasos:
- **Se quita el prefijo numérico** (`005 - Plaza Italia` → `PLAZA ITALIA`; también formatos como `131- HOSPITAL` o `048 -PERON`). Sin esto, el mismo nombre de 2016 y de 2024 nunca sería igual. El número ya no aporta: el id es la clave.
- **Se quitan las tildes** (`Pacífico` → `PACIFICO`). En 2016 vienen con tilde y en 2024 sin tilde.
- **Nombres provisorios a nulo:** `KP1808000368`, `ERRONEA`, `PLANEADA`, `OFF BRAUDACCO`, `TEST TECNICO PDM` aparecen en 2019 como nombre de estación, pero son placeholders del sistema. Si se dejaran, dim_estacion le pondría "KP1808000368" a la estación de Puerto Madero UCA (puesto 8 del top 2016). Los viajes se conservan (20.436), solo pierden el nombre. dim_estacion toma el nombre válido más reciente; los ids 1 y 178, que solo tienen placeholders, quedan como `SIN NOMBRE`.

**D6. `""` y `"NA"` son nulos.** Al pasar a silver, el texto vacío (como lo guarda bronze, D1) y el literal `"NA"` (edades de 2019) se convierten en nulo antes de tipar.

**D7. Regla de duplicados sobre todas las filas.** `DUPLICADO` se evalúa con `duplicated(keep="first")` sobre los tres
años concatenados en orden 2016 → 2019 → 2024 y en el orden de cada archivo. Solo hay un caso: el viaje 22425357 de 2024, que aparece dos veces idéntico.

**D8. Se descartan columnas que no están en el esquema canónico:** coordenadas, direcciones, modelo de bicicleta,
`periodo` y el género que traen los archivos de viajes. El género se toma de la tabla de usuarios, como define el esquema.

**D9. Reglas de calidad que no agregué, aunque las evalué:**
- 37.618 viajes de 2024 tienen duración 0, probablemente retiros cancelados. No viola ninguna regla del enunciado y se conservan. Sería una buena regla futura (`DURACION_CERO`).
- Algunos nombres traen marcas como `-- CDO CHACARITA -- (TEMPORAL)`. Se dejan tal cual.

### Silver: usuarios

**D10. Edades fuera de [16, 90] → nulo** (lo pide el enunciado). Afectó 140 registros, más las 765 edades `"NA"` de 2019. No se rechaza al usuario: su género y fecha de alta siguen siendo válidos.

**D11. Ids de usuario repetidos entre años.** 5.231 ids aparecen en el archivo de 2016 y en el de 2019. Según el
enunciado queda el de `fecha_alta` más reciente (el de 2019). Hay evidencia de que **no son la misma persona**: el
género coincide solo en el 39 % de los casos y las edades no tienen relación. Probablemente el sistema nuevo de 2019 reutilizó ids.
No se puede resolver con estos datos, así que se aplica la regla y se documenta.

**D12. Cuarentena también para usuarios** (`silver/usuarios_cuarentena`). El enunciado solo la pide para viajes, pero el
principio de no perder registros vale igual. Así también se puede conciliar: 320.158 en bronze = 314.927 válidos + 5.231 duplicados.

### Gold

**D13. `dim_fecha` es un calendario continuo** del 2016-01-01 al 2024-12-31 (3.288 días), incluidos 2017, 2018 y 2020 a 2023, que no tienen viajes. Es la práctica estándar: un día sin viajes también existe y debe poder contarse como 0.

**D14. Viajes con `id_usuario` que no está en dim_usuario: 6.120.635 de 9.763.068 viajes con usuario (62,7 %).**
Por año: 2019 tiene 4.410.500 (71 %) y 2024 tiene 1.710.135 (48 %). Además, los 503.252 viajes válidos de 2016 no traen id de usuario.
**Explicación:** cada archivo de usuarios contiene solo las **altas de ese año**. Un usuario que se registró en 2015, 2017,
2018 o entre 2020 y 2023 viaja, pero no está en ningún archivo. Esto cuadra con que en 2024 el 32 % de los viajes es de usuarios dados de alta en 2024, y otro 20 % de usuarios de los archivos de 2016 y 2019.
No se crean filas "desconocido" en dim_usuario. El enunciado acepta que la clave sea nula o no tenga par.

**D15. Rango etario:** 16-25, 26-35, 36-50, 51+ (bordes incluidos). Edad nula → `DESCONOCIDO`.

### Reportes

**D16. Reporte 2:** los empates se rompen por `id_estacion` ascendente, para que el ranking sea determinístico. Se agregó la columna `id_estacion` además del nombre.

**D17. Reporte 3:** se hace un *inner join* con dim_usuario, así que solo cuentan los 3.642.433 viajes cuyo usuario se conoce. Por eso 2016 no aparece (no tiene id de usuario).

**D18. Reporte 5:** además de las columnas pedidas, se agregó una columna por motivo de cuarentena (`cuarentena_fecha_invalida`, etc.) para ver *por qué* se rechaza cada año.

### Ejecución

**D19. Idempotencia verificada:** `python verificar.py --idempotencia` corre el pipeline dos veces y compara un hash del
contenido de todas las tablas y reportes. La única columna excluida es `_fecha_ingesta`, que por diseño cambia en cada corrida.

**D20. Memoria:** el pipeline lee cada año completo (sin `chunksize`). El pico fue de unos 4,3 GB de RAM. Silver lee de bronze solo las columnas que necesita, aprovechando que Parquet es columnar.
