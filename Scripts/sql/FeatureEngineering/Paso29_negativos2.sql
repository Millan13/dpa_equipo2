/*Paso 29: Modificar negativos 2*/
CREATE TABLE RAW.NW27 AS
SELECT *,
CASE WHEN COUNT2 < 0 THEN 0 ELSE COUNT2 END AS COUNT3
FROM RAW.NW26
;





