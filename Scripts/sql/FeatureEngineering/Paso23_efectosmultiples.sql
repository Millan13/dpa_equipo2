/*Paso 23: Eliminar cuando ocurren dobles o triples efectos domino*/
CREATE TABLE RAW.NW21 AS
SELECT *
FROM RAW.NW20 
WHERE SUM_EFECTOS_DOMINO <= 1
ORDER BY ID_AVION, FECHA, HORASALIDAF
;