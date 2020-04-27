/*Paso 7: Continuacion - Crear la fecha de vuelo*/
CREATE TABLE RAW.NW6 AS
SELECT *, to_date(FECHA_VUELO, 'ddmmyyyy') AS FECHA
FROM RAW.NW5
;
