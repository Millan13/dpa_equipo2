/*Paso 16: Jugada para distraer a la maquina*/
CREATE TABLE RAW.NW14 AS
SELECT *, CASE WHEN rank_contador = 1 THEN null ELSE IND_RETRASO2 END AS IND_RETRASO3
FROM RAW.NW13
;
