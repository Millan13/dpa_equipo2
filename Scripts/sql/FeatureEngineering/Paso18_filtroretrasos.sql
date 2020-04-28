/*Aqui me quede-------------------------------------------------------------*/
/*Paso 18: Filtrar la tabla para quedarse con puros retrasos*/
/*Filtrar (la tabla queda de  2'454,328) donde Ind_Retraso1 = 1
(para quedarme con todos los vuelos retrasados).*/
DROP TABLE IF EXISTS TRANSFORM.NW16;
CREATE TABLE TRANSFORM.NW16 AS
SELECT *
FROM TRANSFORM.NW15 
WHERE IND_RETRASO1 = 1
;
