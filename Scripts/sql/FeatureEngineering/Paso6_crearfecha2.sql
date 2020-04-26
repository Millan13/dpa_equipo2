/*Paso 6: Continuacion - Crear la fecha de vuelo*/
CREATE TABLE RAW.NW5 AS
SELECT * , CONCAT(DAY,MONTH,YEAR) AS FECHA_VUELO
FROM RAW.NW4
;
