/*Paso 13: Hacer la resta para obtener el numero de vuelos faltantes en el dia para el mismo avion*/
/*SELECT *, COALESCE(col1,0) + COALESCE(col2,0) AS NVUE_FALT Buen aprendizaje ese coalesce*/
CREATE TABLE RAW.NW11 AS
SELECT *, (max - rank_contador) AS NVUE_FALT
FROM RAW.NW10
;
