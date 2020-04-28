/*Paso 10: Realizar el conteo de vuelos por Fecha y Id_vuelo*/
CREATE TABLE RAW.NW9 AS
SELECT *, 
	RANK() OVER (
			PARTITION BY ID_AVION, FECHA
			ORDER BY ID_AVION, FECHA, HORASALIDAF) rank_contador
FROM RAW.NW8
;
