/*Paso 13: Hacer la resta para obtener el numero de vuelos faltantes en el dia para el mismo avion*/
/*SELECT *, COALESCE(col1,0) + COALESCE(col2,0) AS NVUE_FALT Buen aprendizaje ese coalesce*/
DROP TABLE IF EXISTS TRANSFORM.NW11;
CREATE TABLE TRANSFORM.NW11 AS
SELECT *, (max - rank_contador) AS NVUE_FALT
FROM TRANSFORM.NW10
;
