/* Paso 1: Quedarnos con operador de vuelo de WN */
CREATE TABLE RAW.NW AS
SELECT *
FROM RAW.VUELOS2 
WHERE OP_UNIQUE_CARRIER = 'WN'
;
