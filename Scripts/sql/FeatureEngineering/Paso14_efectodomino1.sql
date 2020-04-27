/*Paso 14: Se empieza a generar el efecto domino*/
/*Generar bandera de los afectados*/
CREATE TABLE RAW.NW12 AS
SELECT *, CASE WHEN BANDERA_DELAY = 'A:SIN RETRASO' THEN 0 ELSE 1 END AS IND_RETRASO1
FROM RAW.NW11
;
