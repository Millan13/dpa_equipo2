#### Diccionario de campos de tablas de linaje


**ejecuciones**
+ Id_ejec: identificador de cada ejecución realizada.
+ Id_archivo: Identificador único de descarga.
+ Usuario_ejec: Usuario en la instancia que dispara el proceso.
+ Instancia_ejec: Nombre de la instancia donde se ejecuta el proceso.
+ Fecha_hora_ejec: Fecha y hora de la ejecución del proceso.
+ Bucket_S3: Nombre del bucket donde se almacenará los archivos descargados.
+ Ruta_almac_S3: Ruta de almacenamiento dentro de S3.
+ Tag_script: Tag del script utilizado para la ejecución.
+ Tipo_ejec: Si fue la ejecución inicial (único) o la recurrente.
+ Url_webscrapping: Url de la página donde se realiza la extracción de información.
+ Status_ejec: Si fue exitosa o no la ejecución.

**archivos**
+ Id_archivo: Identificador único de descarga.
+ Num_registros: Cantidad de observaciones que contiene el archivo (sin contar el header).
+ Num_columnas: Cantidad de variables que contiene el archivo.
+ Tamanio_archivo:Tamaño del archivo descargado.
+ Anio: Año del archivo descargado.
+ Mes: Mes del archivo descargado.

**archivos_det** (detalles del archivo)
+ Id_archivo: Identificador único de descarga.
+ Id_col: Identificador de las columnas del archivo.

**transform**

**modelling**
