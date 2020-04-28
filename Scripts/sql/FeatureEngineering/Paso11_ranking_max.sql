/*Paso 11: Tomar el maximo rank por Fecha y Id_Vuelo*/
CREATE TABLE RAW.MAX_NW AS
SELECT (MAX(rank_contador)) AS MAX,
       ID_AVION, FECHA
FROM RAW.NW9
GROUP BY ID_AVION, FECHA
ORDER BY ID_AVION, FECHA
;
