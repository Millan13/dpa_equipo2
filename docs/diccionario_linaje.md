#### Diccionario de campos de tablas de linaje


**ejecuciones**
+ id_ejec: identificador de cada ejecución realizada.
+ id_archivo: Identificador único de descarga.
+ usuario_ejec: Usuario en la instancia que dispara el proceso.
+ instancia_ejec: Nombre de la instancia donde se ejecuta el proceso.
+ fecha_hora_ejec: Fecha y hora de la ejecución del proceso.
+ bucket_S3: Nombre del bucket donde se almacenará los archivos descargados.
+ ruta_almac_S3: Ruta de almacenamiento dentro de S3.
+ tag_script: Tag del script utilizado para la ejecución.
+ tipo_ejec: Si fue la ejecución inicial (único) o la recurrente.
+ url_webscrapping: Url de la página donde se realiza la extracción de información.
+ status_ejec: Si fue exitosa o no la ejecución.

**archivos**
+ id_ejec: identificador de cada ejecución realizada.
+ id_archivo: Identificador único de descarga.
+ num_registros: Cantidad de observaciones que contiene el archivo (sin contar el header).
+ num_columnas: Cantidad de variables que contiene el archivo.
+ tamanio_archivo:Tamaño del archivo descargado.
+ anio: Año del archivo descargado.
+ mes: Mes del archivo descargado.
+ ruta_almac_s3: Ruta de almacenamiento dentro del bucket de S3.

**archivos_det** (detalles del archivo)
+ id_archivo: Identificador único de descarga.
+ id_col: Identificador de las columnas del archivo.

**transform**
+ id_set_transform: Identificador de la ejecución en que se realiza.
+ num_seq: Número de query que se ejecuta.
+ nombre_query: Nombre del query que ejecuta.
+ filas_afectadas: Número de filas que se modifican con el query.
+ fecha_hora_ejec: Fecha y hora de la ejecución del query.
+ usuario_ejec: Usuario que ejecuta el query.
+ instancia_ejec: Instancia donde se ejecuta el query.
+ tipo_ejec: Si es entrenamiento o predict.

**modelling**
+ id_set_modelado: Identificador de la ejecución en que se realiza.
+ nombre_modelo: Nombre del modelo que se ejecuta.
+ mejor_score_modelo: Valor del mejor score del modelo.
+ fecha_hora_ejec: Fecha y hora en que se ejecuta el modelo.
+ usuario_ejec: Usuario que ejecuta el modelo.
+ instancia_ejec: Instancia donde se ejecuta el modelo.

**unit_test**
+ id_unit_test: Identificador del unit test que se corrió.
+ nombre_clase: Nombre de la clase que corrió el unit test.
+ nombre_metodo: Nombre del método que corrió el unit test.
+ str_estatus: Mensaje si el unit test fue aceptado o rechazado.
+ str_mensaje: Mensaje de error por el que tronó el unit test.
+ fecha_hora_ejec: Fecha y hora de la ejecución del unit test.

**schedules** (metadata para los datos de predict)
+ id_ejec: identificador de cada ejecución realizada.
+ id_archivo: Identificador único de descarga.
+ num_registros: Cantidad de observaciones que contiene el archivo (sin contar el header).
+ num_columnas: Cantidad de variables que contiene el archivo.
+ tamanio_archivo:Tamaño del archivo descargado.
+ anio: Año del archivo descargado.
+ mes: Mes del archivo descargado.
+ ruta_almac_s3: Ruta de almacenamiento dentro del bucket de S3.

**schedules_de** (metadata para los datos de predict, detalles de schedules)
+ id_archivo: Identificador único de descarga.
+ nombre_col: Nombre de las columnas del archivo.

