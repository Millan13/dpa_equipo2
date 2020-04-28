/*Paso 3: Modificar a numerico algunas columnas para poder operarlas*/
ALTER TABLE TRANSFORM.NW2
ALTER COLUMN DELAY TYPE NUMERIC USING delay::numeric
/*ALTER COLUMN column_name_2 [SET DATA] TYPE new_data_type, AQUI SE PUEDEN PONER MAS VARIABLES*/
;
