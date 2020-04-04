/***********************************************************************************/
/**************************** Preparacion de las tablas ****************************/
/***********************************************************************************/


/**************************** Titulo ejecuciones ****************************/
drop table if exists linaje.ejecuciones;
create table linaje.ejecuciones (
  "id_ejec" VARCHAR(20),
  "id_archivo" VARCHAR(40),
  "usuario_ejec" VARCHAR(20),
  "instancia_ejec" VARCHAR(20),
  --"fecha_hora_ejec" TIMESTAMP,
  "bucket_s3" VARCHAR(40),
  "ruta_almac_s3" VARCHAR(100),
  "tag_script" VARCHAR(20),
  "tipo_ejec" VARCHAR(5),
  "url_webscrapping" VARCHAR(100),
  "status_ejec" VARCHAR(5)
);
comment on table linaje.ejecuciones is 'describe los datos principales de las ejecuciones';


/**************************** Titulo archivos ****************************/
drop table if exists linaje.archivos;
create table linaje.archivos (
  "id_archivo" VARCHAR(40),
  "num_registros" VARCHAR(20),
  "num_columnas" VARCHAR(10),
  "tamanio_archivo" FLOAT,
  "anio" VARCHAR(5),
  "mes" VARCHAR(10)
);
comment on table linaje.archivos is 'describe caracteristicas especificas de archivos';


/**************************** Titulo archivos_det ****************************/
drop table if exists linaje.archivos_det;
create table linaje.archivos_det (
  "id_archivo" VARCHAR(20),
  "id_col" VARCHAR(20)
);
comment on table linaje.archivos_det is 'describe detalles del archivo';


/**************************** Titulo columnas ****************************/
drop table if exists linaje.columnas;
create table linaje.columnas (
  "id_col" VARCHAR(20),
  "etiqueta_col" VARCHAR(20),
  "tipo_col" VARCHAR(20)

);
comment on table linaje.columnas is 'describe informacion de las columnas';
