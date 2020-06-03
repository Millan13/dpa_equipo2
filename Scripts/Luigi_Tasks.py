import os
import numpy as np
import subprocess
from datetime import datetime

# Librerias de nosotros
from Class_Utileria import Utileria

# ###############################################################
# ################### Funciones principales #####################
# ###############################################################


def CrearDB():

    print('\n---Inicio creacion DB ---\n')

    import psycopg2

    objUtileria = Utileria()

    if objUtileria.ExisteBaseCreada():
        print('Ya existe una BD creada')
    else:
        conn = psycopg2.connect(user=objUtileria.str_UsuarioDB,
                                host=objUtileria.str_EndPointDB,
                                password=objUtileria.str_PassDB)

        conn.autocommit = True
        queries = objUtileria.ObtenerQueries('sql')
        query = queries.get('create_db')

        try:
            with conn.cursor() as (cur):
                cur.execute(query)
        except Exception:
            print('Excepcion en CrearDB-cur.execute')
            raise
            return 1

    print('\n---Fin creacion DB ---\n')
    return 0


def CrearDirectoriosEC2():

    print('\n---Inicio creacion directorio EC2 ---\n')

    arr_Directorios = ['Descargas',
                       'Linaje',
                       'Linaje/Ejecuciones',
                       'Linaje/Archivos',
                       'Linaje/ArchivosDet',
                       'Linaje/Transform',
                       'Linaje/Modeling',
                       'Linaje/Schedules',
                       'Linaje/SchedulesDet',
                       'Linaje/BiasFairness',
                       'Linaje/Predict',
                       'testing/Extract',
                       'testing/Load',
                       'testing/Transform',
                       'testing/Modeling',
                       'testing/Predict']

    try:
        # Barremos para eliminar los directorios
        for strDirectorio in arr_Directorios:
            str_Comando = 'rm -r '+strDirectorio
            os.system(str_Comando)

        # Barremos para crear los directorios
        for strDirectorio in arr_Directorios:
            os.mkdir(strDirectorio)
            print('Directorio:', strDirectorio, ' creado ')

    except FileExistsError:
        print('Excepcion en CrearDirectoriosS3-put_object():')
        raise
        return 1

    print('\n---Fin creacion directorio EC2 ---\n')
    return 0


def CrearDirectoriosS3():

    print('\n---Inicio creacion directorio S3--- \n')

    # import boto3
    from Class_Rita import Rita

    objUtileria = Utileria()
    objRita = Rita()

    arr_Anios = objRita.ObtenerAnios()
    arr_Meses = objRita.ObtenerMeses()

    cnx_S3 = objUtileria.CrearConexionS3()
    for anio in arr_Anios:
        print('anio: ', anio)
        for mes in arr_Meses:
            print('mes: ', mes)
            directory_name = 'carga_inicial/' + str(anio) + '/' + str(mes)
            print('directory_name: ', directory_name)

            try:
                cnx_S3.put_object(Bucket=objUtileria.str_NombreBucket, Key=(directory_name+'/'))
            except Exception:
                print('Excepcion en CrearDirectoriosS3-put_object():')
                raise
                return 1

    directory_name = 'carga_recurrente'
    print('directory_name: ', directory_name)
    try:
        cnx_S3.put_object(Bucket=objUtileria.str_NombreBucket, Key=(directory_name + '/'))
    except Exception:
        print('Excepcion en CrearDirectoriosS3-put_object():')
        raise
        return 1

    directory_name = 'modelo_seleccionado'
    print('directory_name: ', directory_name)
    try:
        cnx_S3.put_object(Bucket=objUtileria.str_NombreBucket, Key=(directory_name + '/'))
    except Exception:
        print('Excepcion en CrearDirectoriosS3-put_object():')
        raise
        return 1

    directory_name = 'schedule_vuelos'
    print('directory_name: ', directory_name)
    try:
        cnx_S3.put_object(Bucket=objUtileria.str_NombreBucket, Key=(directory_name + '/'))
    except Exception:
        print('Excepcion en CrearDirectoriosS3-put_object():')
        raise
        return 1

    print('---Fin creacion directorio S3---\n')
    return 0


def CrearSchemasRDS():
    print('---Inicio creacion schemas---\n')
    objUtileria = Utileria()
    conn = objUtileria.CrearConexionRDS()
    conn.autocommit = True
    queries = objUtileria.ObtenerQueries('sql')
    query = queries.get('create_schemas')

    try:
        with conn.cursor() as (cur):
            cur.execute(query)
    except Exception:
        print('Excepcion en CrearSchemasRDS-cur.execute')
        raise
        return 1

    print('---Fin creacion schemas---\n')
    return 0


def CrearTablasRDS():
    print('---Inicio creacion tablas---\n')
    objUtileria = Utileria()
    conn = objUtileria.CrearConexionRDS()
    conn.autocommit = True
    queries = objUtileria.ObtenerQueries('sql')
    query = queries.get('create_tables')

    try:
        with conn.cursor() as (cur):
            cur.execute(query)
    except Exception:
        print('Excepcion en CrearTablasRDS-cur.execute')
        raise
        return 1

    print('---Fin creacion tablas---\n')
    return 0


def WebScrapingInicial():

    print('\n---Inicio web scraping Inicial---')
    # import glob, os, time
    from Class_Rita import Rita
    from Class_ValueObjects import voEjecucion
    from Class_ValueObjects import voArchivos
    from Class_ValueObjects import voArchivos_Det
    import platform

    objUtileria = Utileria()
    objWebScraping = Rita()
    arr_Anios = objWebScraping.ObtenerAnios()
    arr_Meses = objWebScraping.ObtenerMeses()

    voEjecucion = voEjecucion()
    voArchivo = voArchivos()

    # Se obtiene el id de ejecución
    conn = objUtileria.CrearConexionRDS()
    nbr_Id_Ejec_Actual = objUtileria.ObtenerMaxId(conn,
                                                  'linaje.ejecuciones',
                                                  'id_ejec') + 1
    for anio in arr_Anios:
        print('anio: ', anio)
        for mes in arr_Meses:
            print('mes: ', mes)

            try:
                objWebScraping.DescargarAnioMes(anio, mes)
            except Exception:
                print('Excepcion en WebScrapingInicial-DescargarAnioMes')
                raise
                return 1

            if objWebScraping.str_ArchivoDescargado != '':
                print('Descarga completa')
                print('objWebScraping.str_ArchivoDescargado: ',
                      objWebScraping.str_ArchivoDescargado)
                os.system("unzip 'Descargas/*.zip' -d Descargas/")
                os.system('rm Descargas/*.zip')
                cnx_S3 = objUtileria.CrearConexionS3()
                str_ArchivoLocal = 'Descargas/' + os.path.basename(objWebScraping.str_ArchivoDescargado + '.csv')
                str_RutaS3 = 'carga_inicial/' + str(anio) + '/' + mes + '/'

                # Mandamos el archivo descargado a S3
                try:
                    objUtileria.MandarArchivoS3(cnx_S3, objUtileria.str_NombreBucket, str_RutaS3, str_ArchivoLocal)
                    # print('Se omite el envio a S3')
                except Exception:
                    print('Excepcion en MandarArchivoS3')
                    raise
                    return 1

                if platform.system()=='Darwin':
                    os.system("sed -i '' 's/.$//' Descargas/*.csv")
                else:
                    os.system("sed -i 's/.$//' Descargas/*.csv")

                # ####### método 1
                cnn = objUtileria.CrearConexionRDS()

                # Mandamos la información raw del archivo al RDS
                # print('Se omite el envio a RDS')
                data_file = open(str_ArchivoLocal, "r")
                objUtileria.InsertarEnRDSDesdeArchivo2(cnn, data_file, 'raw.historico')

                # Antes de eliminar los archivos que ya fueron enviados a S3,
                # obtenemos información de ellos
                nbr_Tamanio = objUtileria.ObtenerTamanioArchivo(objWebScraping.str_ArchivoDescargado + '.csv')
                nbr_Filas = len(open(objWebScraping.str_ArchivoDescargado + '.csv').readlines())

                # Se elimina la información descargada
                os.system('rm Descargas/*.csv')

                # CSV Linaje.Archivos
                voArchivo.nbr_id_ejec = nbr_Id_Ejec_Actual
                voArchivo.str_id_archivo = os.path.basename(objWebScraping.str_ArchivoDescargado + '.csv')
                voArchivo.nbr_tamanio_archivo = nbr_Tamanio
                voArchivo.nbr_num_registros = nbr_Filas

                # Se filtra el diccionario para traer solo campos de activacion
                dict_Filtrado = {k: v for k, v in objWebScraping.dict_Campos.items() if v['Flag'] == 'A'}
                voArchivo.nbr_num_columnas = len(dict_Filtrado)

                voArchivo.str_anio = str(anio)
                voArchivo.str_mes = str(mes)
                voArchivo.str_NombreDataFrame = 'Linaje/Archivos/' + str(anio) + str(mes) + '.csv'
                voArchivo.str_ruta_almac_s3 = str_RutaS3
                voArchivo.crearCSV()

                # CSV Linaje.Archivos_Det
                # Obtenemos los nombres de columnas del diccionario
                # y los ponemos en un arreglo
                voArchivo_Det = voArchivos_Det()
                np_Campos = np.empty([0, 2])
                for key, value in objWebScraping.dict_Campos.items():

                    # Se pregunta si el campo esta marcado para activarse
                    if value['Flag'] == 'A':
                        np_Campos = np.append(np_Campos, [[voArchivo.str_id_archivo, key]], axis=0)

                voArchivo_Det.np_Campos = np_Campos
                voArchivo_Det.str_NombreDataFrame = 'Linaje/ArchivosDet/' + str(anio) + str(mes) + '.csv'
                voArchivo_Det.crearCSV()

    # CSV Linaje.Ejecuciones
    voEjecucion.nbr_id_ejec = nbr_Id_Ejec_Actual
    voEjecucion.str_bucket_s3 = objUtileria.str_NombreBucket
    voEjecucion.str_usuario_ejec = objUtileria.ObtenerUsuario()
    voEjecucion.str_instancia_ejec = objUtileria.ObtenerIp()
    voEjecucion.str_tipo_ejec = 'CI'
    voEjecucion.str_url_webscrapping = objWebScraping.str_Url
    voEjecucion.str_status_ejec = 'Ok'
    voEjecucion.dttm_fecha_hora_ejec = datetime.now()
    voEjecucion.str_tag_script =str(subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']))[2:-3]
    voEjecucion.str_NombreDataFrame = 'Linaje/Ejecuciones/' \
                                       + voEjecucion.str_tipo_ejec + '_' \
                                       + str(voEjecucion.nbr_id_ejec) + '.csv'
    voEjecucion.crearCSV()

    print('---Fin web scraping Inicial---\n')
    return 0


def EnviarMetadataLinajeCargaRDS():
    print('\n---Inicio envio de linaje carga---\n')
    from pathlib import Path
    objUtileria = Utileria()
    cnn = objUtileria.CrearConexionRDS()
    cnn.autocommit = True

    # Barremos los csv de Ejecuciones
    for data_file in Path('Linaje/Ejecuciones').glob('*.csv'):

        try:
            objUtileria.InsertarEnRDSDesdeArchivo(cnn, data_file, 'linaje.ejecuciones')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeCargaRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-ejecuciones
    os.system('rm Linaje/Ejecuciones/*.csv')

    # Barremos los csv de Archivos
    for data_file in Path('Linaje/Archivos').glob('*.csv'):

        try:
            objUtileria.InsertarEnRDSDesdeArchivo(cnn, data_file, 'linaje.archivos')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeCargaRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-archivos
    os.system('rm Linaje/Archivos/*.csv')

    # Barremos los csv de ArchivosDet
    for data_file in Path('Linaje/ArchivosDet').glob('*.csv'):

        try:
            objUtileria.InsertarEnRDSDesdeArchivo(cnn, data_file, 'linaje.archivos_det')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeCargaRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-archivosdet
    os.system('rm Linaje/ArchivosDet/*.csv')


    # Barremos los csv de Schedules
    for data_file in Path('Linaje/Schedules').glob('*.csv'):

        try:
            objUtileria.InsertarEnRDSDesdeArchivo(cnn, data_file, 'linaje.Schedules')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeCargaRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-Schedules
    os.system('rm Linaje/Schedules/*.csv')

    # Barremos los csv de SchedulesDet
    for data_file in Path('Linaje/SchedulesDet').glob('*.csv'):

        try:
            objUtileria.InsertarEnRDSDesdeArchivo(cnn, data_file, 'linaje.Schedules_det')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeCargaRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-Schedulesdet
    os.system('rm Linaje/SchedulesDet/*.csv')

    print('\n---Fin envio de linaje carga---\n')
    return 0


def EnviarMetadataLinajeTransformRDS():
    print('\n---Inicio envío de linaje transform ---\n')
    from pathlib import Path
    objUtileria = Utileria()
    cnn = objUtileria.CrearConexionRDS()
    cnn.autocommit = True

    # Barremos los csv de Ejecuciones
    for data_file in Path('Linaje/Transform').glob('*.csv'):

        try:
            objUtileria.InsertarEnRDSDesdeArchivo(cnn, data_file, 'linaje.transform')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeTransformRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-archivosdet
    os.system('rm Linaje/Transform/*.csv')

    print('\n---Fin envío de linaje transform---\n')
    return 0


def WebScrapingRecurrente():

    print('\n---Inicio web scraping recurrente---')
    # import glob, os, time
    from Class_Rita import Rita
    from Class_ValueObjects import voEjecucion
    from Class_ValueObjects import voArchivos
    from Class_ValueObjects import voArchivos_Det

    objUtileria = Utileria()
    objRita = Rita()
    voEjecucion = voEjecucion()
    voArchivo = voArchivos()
    veces_descargado = 0

    # Se obtiene el id de ejecución
    conn = objUtileria.CrearConexionRDS()
    nbr_Id_Ejec_Actual = objUtileria.ObtenerMaxId(conn,
                                                  'linaje.ejecuciones',
                                                  'id_ejec') + 1

    # Extraemos el último mes y año disponibles para descarga
    latest = objRita.ObtenerMesDescargaRecurrente()
    latest_date = latest.split(" ")  # Separamos el mes y el año por espacio
    anio =  latest_date[1]
    mes  =  latest_date[0]
    print('anio: ', anio)
    print('mes: ', mes)

    # Query para verificar si ya se ha descargado el último mes disponible
    query = "select * from linaje.archivos where anio='"+anio+"' and mes='"+mes+"';"
    veces_descargado = objUtileria.EjecutarQuery(conn, query)
    print('Los datos de',mes,anio,'habian sido descargados',veces_descargado,'veces.')

    if veces_descargado < 1:

        try:
            objRita.DescargarAnioMes(anio, mes)
        except Exception:
            print('Excepcion en WebScrapingRecurrente-DescargarAnioMes')
            raise
            return 1

        if objRita.str_ArchivoDescargado != '':
            print('Descarga completa')
            print('objRita.str_ArchivoDescargado: ',
                  objRita.str_ArchivoDescargado)
            os.system("unzip 'Descargas/*.zip' -d Descargas/")
            os.system('rm Descargas/*.zip')
            cnx_S3 = objUtileria.CrearConexionS3()
            str_ArchivoLocal = 'Descargas/' + os.path.basename(objRita.str_ArchivoDescargado + '.csv')
            str_RutaS3 = 'carga_recurrente/' + str(anio) + '/' + mes + '/'

            try:
                objUtileria.MandarArchivoS3(cnx_S3, objUtileria.str_NombreBucket, str_RutaS3, str_ArchivoLocal)
                # print('Se omite el envio')
            except Exception:
                print('Excepcion en MandarArchivoS3')
                raise
                return 1

            # Antes de eliminar los archivos que ya fueron enviados a S3,
            # obtenemos información de ellos
            nbr_Tamanio = objUtileria.ObtenerTamanioArchivo(objRita.str_ArchivoDescargado + '.csv')
            nbr_Filas = len(open(objRita.str_ArchivoDescargado + '.csv').readlines())

            # Se elimina la información descargada
            os.system('rm Descargas/*.csv')

            # CSV Linaje.Archivos
            voArchivo.nbr_id_ejec = nbr_Id_Ejec_Actual
            voArchivo.str_id_archivo = os.path.basename(objRita.str_ArchivoDescargado + '.csv')
            voArchivo.nbr_tamanio_archivo = nbr_Tamanio
            voArchivo.nbr_num_registros = nbr_Filas

            # Se filtra el diccionario para traer solo campos de activacion
            dict_Filtrado = {k: v for k, v in objRita.dict_Campos.items() if v['Flag'] == 'A'}
            voArchivo.nbr_num_columnas = len(dict_Filtrado)

            voArchivo.str_anio = str(anio)
            voArchivo.str_mes = str(mes)
            voArchivo.str_NombreDataFrame = 'Linaje/Archivos/' + str(anio) + str(mes) + '.csv'
            voArchivo.str_ruta_almac_s3 = str_RutaS3
            voArchivo.crearCSV()

            # CSV Linaje.Archivos_Det
            # Obtenemos los nombres de columnas del diccionario
            # y los ponemos en un arreglo
            voArchivo_Det = voArchivos_Det()
            np_Campos = np.empty([0, 2])
            for key, value in objRita.dict_Campos.items():

                # Se pregunta si el campo esta marcado para activarse
                if value['Flag'] == 'A':
                    np_Campos = np.append(np_Campos, [[voArchivo.str_id_archivo, key]], axis=0)

            voArchivo_Det.np_Campos = np_Campos
            voArchivo_Det.str_NombreDataFrame = 'Linaje/ArchivosDet/' + str(anio) + str(mes) + '.csv'
            voArchivo_Det.crearCSV()

        # CSV Linaje.Ejecuciones
        voEjecucion.nbr_id_ejec = nbr_Id_Ejec_Actual
        voEjecucion.str_bucket_s3 = objUtileria.str_NombreBucket
        voEjecucion.str_usuario_ejec = objUtileria.ObtenerUsuario()
        voEjecucion.str_instancia_ejec = objUtileria.ObtenerIp()
        voEjecucion.str_tipo_ejec = 'R'
        voEjecucion.str_url_webscrapping = objRita.str_Url
        voEjecucion.str_status_ejec = 'Ok'
        voEjecucion.dttm_fecha_hora_ejec = datetime.now()
        voEjecucion.str_tag_script =str(subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']))[2:-3]
        voEjecucion.str_NombreDataFrame = 'Linaje/Ejecuciones/' \
                                           + voEjecucion.str_tipo_ejec + '_' \
                                           + str(voEjecucion.nbr_id_ejec) + '.csv'
        voEjecucion.crearCSV()

    print('---Fin web scraping recurrente---\n')
    return 0


def HacerFeatureEngineering(str_tipo_ejecucion):
    print('---Inicio de feature engineering---\n')

    objUtileria = Utileria()

    # Se deben leer los querys que se van a ejecutar y ejecutarlos
    objUtileria = Utileria()
    conn = objUtileria.CrearConexionRDS()
    conn.autocommit = True
    queries = objUtileria.ObtenerQueries('sql/FeatureEngineering')

    # Variables generales para el linaje
    nbr_IdSet = objUtileria.ObtenerMaxId(conn,
                                         'linaje.transform',
                                         'id_set_transform') + 1
    str_Ruta = 'Linaje/Transform/'


    if str_tipo_ejecucion == 'train':
        # Query 1 train
        str_NombreQuery = 'Paso0_copytable'
        query = queries.get(str_NombreQuery)
        nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
        CrearMetadataTrans(nbr_IdSet, 1, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)
    elif str_tipo_ejecucion == 'predict':
        # Query 1 predict
        str_NombreQuery = 'Paso0_copytable_recurrente'
        query = queries.get(str_NombreQuery)
        nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
        CrearMetadataTrans(nbr_IdSet, 1, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 1
    #str_NombreQuery = 'Paso0_copytable'
    #query = queries.get(str_NombreQuery)
    #nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    #CrearMetadataTrans(nbr_IdSet, 1, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 2
    str_NombreQuery = 'Paso1_filtroWN'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 2, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 3
    str_NombreQuery = 'Paso2_rename'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 3, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 4
    str_NombreQuery = 'Paso3_delaynumeric'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 4, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 4_5
    str_NombreQuery = 'Paso3_5_delaypositive'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 5, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 5
    str_NombreQuery = 'Paso4_banderadelay'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 6, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 6
    str_NombreQuery = 'Paso5_crearfecha1'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 7, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 7
    str_NombreQuery = 'Paso6_crearfecha2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 8, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 8
    str_NombreQuery = 'Paso7_crearfecha2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 9, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 9
    str_NombreQuery = 'Paso8_tantitalimpieza'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 10, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 10
    str_NombreQuery = 'Paso9_ordenar'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 11, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 11
    str_NombreQuery = 'Paso10_conteo'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 12, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 12
    str_NombreQuery = 'Paso11_ranking_max'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 13, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 13
    str_NombreQuery = 'Paso12_innerjoin'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 14, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 14
    str_NombreQuery = 'Paso13_vuelosfaltantes'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 15, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 15
    str_NombreQuery = 'Paso14_efectodomino1'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 16, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 16
    str_NombreQuery = 'Paso15_efectodomino2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 17, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 17
    str_NombreQuery = 'Paso16_distraccion'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 18, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 18
    str_NombreQuery = 'Paso17_banderadomino'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 19, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 19
    str_NombreQuery = 'Paso18_filtroretrasos'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 20, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 20
    str_NombreQuery = 'Paso19_lagbandera'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 21, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 21
    str_NombreQuery = 'Paso20_vueloculpable'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 22, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 22
    str_NombreQuery = 'Paso21_efectodomino3'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 23, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 23
    str_NombreQuery = 'Paso22_regresoretrasos'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 24, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 24
    str_NombreQuery = 'Paso23_efectosmultiples'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 25, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 25
    str_NombreQuery = 'Paso24_totaldomino'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 26, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 26
    str_NombreQuery = 'Paso25_regresoretrasos2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 27, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 27
    str_NombreQuery = 'Paso26_negativos1'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 28, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 28
    str_NombreQuery = 'Paso27_contadorinverso'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 29, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 29
    str_NombreQuery = 'Paso28_contadorinverso2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 30, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 30
    str_NombreQuery = 'Paso29_negativos2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 31, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 31
    str_NombreQuery = 'Paso30_regresoretrasos3'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 32, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)

    # Query 32
    str_NombreQuery = 'Paso31_etiqueta'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 33, str_NombreQuery, nbr_FilasAfec, str_Ruta, str_tipo_ejecucion)
    # Aquí se deben de poner el resto de queries del feature engineering

    # Se genera el CSV que servirá para el modelado:
    str_Query1 = 'SELECT * FROM TRANSFORM.NWFINAL'
    str_Query2 = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(str_Query1)

    str_NombreArch = 'DatasetModelado.csv'

    db_cursor = conn.cursor()
    with open(str_NombreArch, 'w') as file:
        db_cursor.copy_expert(str_Query2, file)

    import pandas as pd

    if str_tipo_ejecucion == 'predict':
        #df_DataSetModelado = pd.read_csv('DatasetModelado.csv',
        #                                sep = ',',
        #                                nrows = 5000
        #                                )
        #df_DataSetModelado.to_csv('DatasetModelado.csv',
        #                          sep = ','
        #
        #                   )
        print('--1')
        df = pd.read_csv('DatasetModelado.csv', sep = ',')
        filas = df.shape[0]
        porcion = int(filas/10)
        df2 = df.drop(df.index[porcion:filas])
        df2 = df2.drop(['etiqueta1'], axis=1)

        df2.to_csv('DatasetModeladoPredict.csv', sep=',', index=False)
        print('--2')
    print('---Fin de feature engineering---\n')

    return 0


def Modelar():
    print('---Inicio de Modelar ---\n')

    from Class_Rita import Rita
    import pickle as pickle

    objRita = Rita()

    # objRita.Modelar('Transit_modeling.csv')
    objRita.Modelar('DatasetModelado.csv')

    # Se guarda el pickle para usarse en aequitas
    pickleFile = open('X_testRita.pickle', 'wb')
    pickle.dump(objRita.ObjEda.X_test, pickleFile)
    pickleFile.close()

    # Se guarda el pickle para usarse en aequitas
    pickleFile = open('Y_testRita.pickle', 'wb')
    pickle.dump(objRita.ObjEda.Y_test, pickleFile)
    pickleFile.close()


    # Se guarda el pickle del modelo ganador
    pickleFile = open('parametros.pickle', 'wb')
    pickle.dump(objRita.ModeloGanadorMagicLoop, pickleFile)
    pickleFile.close()

    # Se hace el envío a S3
    EnviarPickleAS3()

    print('---Fin de Modelar---\n')

    return 0


def EnviarMetadataModelingRDS():
    print('\n---Inicio envío metadata modeling ---\n')
    from pathlib import Path
    objUtileria = Utileria()
    cnn = objUtileria.CrearConexionRDS()
    cnn.autocommit = True

    # Barremos los csv de Ejecuciones
    for data_file in Path('Linaje/Modeling').glob('*.csv'):

        try:
            objUtileria.InsertarEnRDSDesdeArchivo(cnn, data_file, 'linaje.modeling')
        except Exception:
            print('Excepcion en EnviarMetadataModelingRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-archivosdet
    os.system('rm Linaje/Modeling/*.csv')
    print('\n---Fin envío metadata modeling---\n')
    return 0


def BiasAndFairness():
    print('\n---Inicio de BiasAndFairness---\n')

    ### Carga modelo y preparación de tabla aequitas
    import sys
    # sys.path.append('/Users/Marco/miniconda3/envs/dpa-rita/lib/python3.8/site-packages')
    # sys.path.append('/Library/Frameworks/Python.framework/Versions/3.7/lib/python3.7/site-packages')

    import numpy as np
    import pickle as pickle
    import pandas as pd
    from Class_Eda import Eda
    from sklearn.base import BaseEstimator, ClassifierMixin
    import aequitas
    import seaborn as sns
    from aequitas.group import Group
    from aequitas.bias import Bias
    from aequitas.fairness import Fairness
    from aequitas.plotting import Plot
    from aequitas.preprocessing import preprocess_input_df
    from aequitas.group import Group

    #Instanciamos el objeto Eda
    objEda = Eda()
    #Inicializamos los parámetros principales (por el momento, sólo es uno: la ruta de la fuente de datos)
    objEda.strRutaDataSource='DatasetModelado2.csv' #El archivo que sale del feature engineering
    #Proceso de carga
    objEda.strSeparadorColumnas = ','
    objEda.Cargar_Datos()
    #Proceso de limpieza
    objEda.Limpiar_Datos()
    #Guardamos el arreglo en la nueva columna
    objEda.pdDataSet['y'] = objEda.pdDataSet.apply(lambda x: (x.etiqueta1), axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['etiqueta1'], axis=1)

    ################## Eliminamos las columnas
    objEda.pdDataSet = objEda.pdDataSet.drop(['fecha'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['id_operador'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['salida_realf'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['bandera_delay'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['ind_retraso2'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['ind_retraso3'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['sum_efectos_domino'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['tot_sum_domino'], axis=1)

    objEda.pdDataSet = objEda.pdDataSet.drop(['tiempo_trans_vuelo'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['distancia_millas'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['delay2'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['ind_retraso1'], axis=1)

    objEda.pdDataSet = objEda.pdDataSet.drop(['efecto'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['year'], axis=1)

    #Variables a incluir que se eliminan en esta prueba:
    objEda.pdDataSet = objEda.pdDataSet.drop(['horasalidaf'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['hora_llegada_progf'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['num_vuelo'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['id_avion'], axis=1)

    objEda.npLabelEncoderFeat=np.array([])
    objEda.Agregar_Features_LabelEnc('day_sem')
    objEda.Agregar_Features_LabelEnc('origen')
    objEda.Agregar_Features_LabelEnc('destino')
    #objEda.Agregar_Features_LabelEnc('anticonceptivo_config')
    #objEda.Agregar_Features_LabelEnc('ocupacion_config')
    #objEda.Agregar_Features_LabelEnc('desc_servicio')
    #objEda.Agregar_Features_LabelEnc('procile')

    #Mostramos el dataSet Auxiliar para ver que aún no ocurre ningún cambio
    objEda.LabelEncoder_OneHotEncoder()

    objEda.Borrar_Cols_Base_LabelEnc()
    objEda.Borrar_Cols_Inter_LabelEnc()

    ################## Separamos las features de lo que vamos a predecir
    pdX, pdY = objEda.SepararFeaturesYPred('y')
    objEda.Generar_Train_Test(pdX, pdY, 0.2)
    ################## Preparamos las variables que imputaremos
    objEda.listTransform=[''] #Limpiamos la propiedad de lista de features a imputar
    objEda.Agregar_Features_Transform('median', 'vuelos_afectados') #no hizo nada porque están como NaN

    ################## Imputamos sobre el conjunto de entrenamiento y prueba
    objEda.X_train = objEda.Imputar_Features(objEda.X_train)
    objEda.X_test = objEda.Imputar_Features(objEda.X_test)

    ## Feature Selection
    ################## Random Forest
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.feature_selection import SelectFromModel

    # Se crea el clasificador Random Forest
    clf = RandomForestClassifier(n_estimators=10, random_state=0, n_jobs=-1)

    # Se entrea al clasificador
    clf.fit(objEda.X_train, objEda.Y_train)

    # Se imprimen los nombres y "gini importance" para cada variable
    #for feature in zip(objEda.pdDataSet.columns, clf.feature_importances_):
    #    print(feature)

    # Se crea un objecto selector que utilizará el clasificador random forest para
    # identificar variables que tienen una importancia mayor a cierto valor

    sfm = SelectFromModel(clf, threshold=0.01)

    # Se entrena al selector
    sfm.fit(objEda.X_train, objEda.Y_train)

    arrIndiceFeaturesAdec = sfm.get_support(indices=True)
    pdX.iloc[:5,arrIndiceFeaturesAdec]
    pdX=pdX.iloc[:,arrIndiceFeaturesAdec]
    #Se eligen las variables para el feature Selection
    ################## Separamos nuestros datos en entrenamiento y pruebas utilizando la proporción 80-20
    objEda.Generar_Train_Test(pdX, pdY, 0.2)
    objEda.X_train = objEda.Imputar_Features(objEda.X_train)
    objEda.X_test = objEda.Imputar_Features(objEda.X_test)
    ################## Imputamos sobre el conjunto de entrenamiento y prueba
    #Importamos modelo
    import pickle as pickle
    # pickleName = 'ModeloFinalRita.p'
    pickleName = 'parametros.pickle'
    pickleFile = open(pickleName, 'rb')
    model = pickle.load(pickleFile)
    pickleFile.close()

    #Hacemos el fit
    # model.fit(X_train, Y_train)
    model.fit(objEda.X_train, objEda.Y_train)

    #Función que realiza las predicciones
    predict_model=lambda x: model.predict_proba(x).astype(float)

    #predict_fn_model = lambda x: model.predict_proba(x).astype(float)
    predict_model

    #Cambiar el nombre de la columnas para Aquitas
    labels_train = ['count','max','nvue_falt','vuelos_afectados','lunes','martes','miercoles','jueves','viernes','sabado','domingo']
    labels_test = labels_train

    X_test = pd.DataFrame(objEda.X_test) #Se convierte en numpy array en Pandas
    X_test.columns=labels_train
    X_test.head()

    #Renombramos las variables según la estructura el input data
    predicciones=pd.DataFrame(model.predict(X_test))
    predicciones=predicciones.rename(columns={0: "score"})

    #Modificaciones a Y_test
    #y_test=pd.DataFrame(Y_test)
    y_test=objEda.Y_test.rename(columns={"y": "label_value"})
    y_test.reset_index(drop=True, inplace=True)

    #Ahora traer la columna de Day_sem (volvemos a cargar datos)
    #Inicializamos los parámetros principales (por el momento, sólo es uno: la ruta de la fuente de datos)
    objEda.strRutaDataSource='DatasetModelado2.csv' #El archivo que sale del feature engineering
    #Proceso de carga
    objEda.strSeparadorColumnas = ','
    objEda.Cargar_Datos()
    #Proceso de limpieza
    objEda.Limpiar_Datos()

    #Guardamos el arreglo en la nueva columna
    objEda.pdDataSet['y'] = objEda.pdDataSet.apply(lambda x: (x.etiqueta1), axis=1)
    pdX_df, pdY_df = objEda.SepararFeaturesYPred('y')
    ################## Separamos las features de lo que vamos a predecir
    objEda.Generar_Train_Test(pdX_df, pdY_df, 0.2)
    ################## Preparamos las variables que imputaremos
    objEda.listTransform=[''] #Limpiamos la propiedad de lista de features a imputar
    objEda.Agregar_Features_Transform('median', 'vuelos_afectados') #no hizo nada porque están como NaN

    ################## Imputamos sobre el conjunto de entrenamiento y prueba
    objEda.X_train = objEda.Imputar_Features(objEda.X_train)
    objEda.X_test = objEda.Imputar_Features(objEda.X_test)

    X_test_df = pd.DataFrame(objEda.X_test)
    #Dejar el día de la semana
    X_test_df = X_test_df[2]
    #Cambiar el nombre de la columnas para Aquitas
    labels_train_df = ['day_sem']
    labels_test_df = labels_train_df

    X_test_df = pd.DataFrame(X_test_df) #Se convierte en numpy array en Pandas
    X_test_df.columns=labels_train_df

    #Unimos los dataframe para generar el input data
    datos_aequitas=pd.concat([predicciones,y_test,X_test,X_test_df], axis=1)
    #Hasta aquí es la preparación de datos para aequitas ------------------------------------------------------------

    #Filtrar los datos de aequitas para calculo de FNR
    datos_aequitas = datos_aequitas[['score','label_value','day_sem']]
    datos_aequitas.head()

    #Instalación de Aequitas
    #pip install aequitas

    g = Group()
    xtab, _ = g.get_crosstabs(datos_aequitas)
    #xtab contiene calculos de todas la métricas de FP, FN, TP, TN

    #Calculo de Bias
    b = Bias()
    bdf = b.get_disparity_predefined_groups(xtab,
                        original_df=datos_aequitas,
                        ref_groups_dict={'day_sem':'e:viernes'},
                        alpha=0.05,
                        check_significance=False)


    #Calculo de Fairness
    f = Fairness()
    fdf = f.get_group_value_fairness(bdf) #Mismo grupo de referencia

    #Exportar archivos
    ruta = "bdf.csv"
    bdf.to_csv(ruta)

    ruta = "fdf.csv"
    fdf.to_csv(ruta)

    print('\n---Fin de BiasAndFairness---\n')
    return 0


def PrepararScheduleVuelos():
    print('\n---Inicio preparación schedule vuelos---\n')

    import boto3
    import os
    import platform
    from Class_Utileria import Utileria

    objUtileria = Utileria()

    path_s3 = 'schedule_vuelos/1016151238_T_ONTIME_REPORTING.csv'
    s3_resource = boto3.resource('s3')
    nombre_bucket = objUtileria.str_NombreBucket
    print('--1')
    # Descarga del archivo de s3 en carpeta Descargas
    #s3_resource.meta.client.download_file(nombre_bucket, path_s3, '/home/ec2-user/dpa_equipo2/Scripts/Descargas/vuelos.csv')
    s3_resource.meta.client.download_file(nombre_bucket, path_s3, 'Descargas/vuelos.csv')
    print('--2')
    file_vuelos = 'Descargas/vuelos.csv'
    print('--3')
    # elimnando comas al final de cada línea del csv
    if platform.system()=='Darwin':
        os.system("sed -i '' 's/.$//' Descargas/*.csv")
    else:
        os.system("sed -i 's/.$//' Descargas/*.csv")

    # envío de la información data_file_vuelos a la RDS
    data_file_vuelos = open(file_vuelos,'r')
    cnn = objUtileria.CrearConexionRDS()
    objUtileria.InsertarEnRDSDesdeArchivo2(cnn,data_file_vuelos,'raw.vuelos')

    # Eliminamos el arhivo de vuelos de Descargas
    os.system('rm Descargas/*.csv')

    print('---Fin preparación schedule vuelos---\n')
    return 0





# ###############################################################
# #################### Funciones de apoyo #######################
# ###############################################################


def CrearMetadataTrans(nbr_IdSet, nbr_seq, str_NombreQuery, nbr_FilasAfectadas, str_Ruta, str_tipo_ejecucion):

    from Class_ValueObjects import voTransform
    objUtileria = Utileria()
    objTransform = voTransform()

    objTransform.nbr_id_set_transform = nbr_IdSet
    objTransform.nbr_num_seq = nbr_seq
    objTransform.str_nombre_query = str_NombreQuery
    objTransform.nbr_filas_afectadas = nbr_FilasAfectadas
    objTransform.dttm_fecha_hora_ejec = datetime.now()
    objTransform.str_usuario_ejec = objUtileria.ObtenerUsuario()
    objTransform.str_instancia_ejec = objUtileria.ObtenerIp()
    objTransform.str_tipo_ejec = str_tipo_ejecucion
    objTransform.str_NombreDataFrame = str_Ruta \
                                     + str(objTransform.nbr_id_set_transform) \
                                     + '_' \
                                     + str(objTransform.nbr_num_seq) + '.csv'


    # print(objTransform.nbr_id_set_transform)
    objTransform.crearCSV()


def EnviarPickleAS3():
    objUtileria = Utileria()

    cnx_S3 = objUtileria.CrearConexionS3()
    str_ArchivoPickleLocal = 'parametros.pickle'
    str_RutaS3 = 'modelo_seleccionado/'

    try:
        objUtileria.MandarArchivoS3(cnx_S3, objUtileria.str_NombreBucket, str_RutaS3, str_ArchivoPickleLocal)
        print("Pickle enviado a S3")
        # print('Se omite el envio')
    except Exception:
        print('Excepcion en EnviarPickleAS3')
        raise
        return 1

    return 0

def WebScrapingScheduleVuelos():

    print('\n---Inicio web scraping Schedule vuelos---')
    # import glob, os, time
    from Class_Rita import Rita
    from Class_ValueObjects import voEjecucion
    from Class_ValueObjects import voArchivos
    from Class_ValueObjects import voArchivos_Det

    objUtileria = Utileria()
    objRita = Rita()
    voEjecucion = voEjecucion()
    voArchivo = voArchivos()
    veces_descargado = 0

    # Se obtiene el id de ejecución
    conn = objUtileria.CrearConexionRDS()
    nbr_Id_Ejec_Actual = objUtileria.ObtenerMaxId(conn,
                                                  'linaje.ejecuciones',
                                                  'id_ejec') + 1

    # Extraemos el último mes y año disponibles para descarga
    latest = objRita.ObtenerMesDescargaRecurrente()
    latest_date = latest.split(" ")  # Separamos el mes y el año por espacio
    anio =  latest_date[1]
    mes  =  latest_date[0]
    print('anio: ', anio)
    print('mes: ', mes)

    # Query para verificar si ya se ha descargado el último mes disponible
    query = "select * from linaje.schedules where anio='"+anio+"' and mes='"+mes+"';"
    veces_descargado = objUtileria.EjecutarQuery(conn, query)
    print('Los datos de',mes,anio,'habian sido descargados',veces_descargado,'veces.')

    if veces_descargado < 1:

        try:
            objRita.DescargarAnioMesSchedules(anio, mes)
        except Exception:
            print('Excepcion en WebScrapingScheduleVuelos-DescargarAnioMesSchedules')
            raise
            return 1

        if objRita.str_ArchivoDescargado != '':
            print('Descarga completa')
            print('objRita.str_ArchivoDescargado: ',
                  objRita.str_ArchivoDescargado)
            os.system("unzip 'Descargas/*.zip' -d Descargas/")
            os.system('rm Descargas/*.zip')
            cnx_S3 = objUtileria.CrearConexionS3()
            str_ArchivoLocal = 'Descargas/' + os.path.basename(objRita.str_ArchivoDescargado + '.csv')
            str_RutaS3 = 'schedule_vuelos/'

            try:
                objUtileria.MandarArchivoS3(cnx_S3, objUtileria.str_NombreBucket, str_RutaS3, str_ArchivoLocal)
                # print('Se omite el envio')
            except Exception:
                print('Excepcion en MandarArchivoS3')
                raise
                return 1

            # Antes de eliminar los archivos que ya fueron enviados a S3,
            # obtenemos información de ellos
            nbr_Tamanio = objUtileria.ObtenerTamanioArchivo(objRita.str_ArchivoDescargado + '.csv')
            nbr_Filas = len(open(objRita.str_ArchivoDescargado + '.csv').readlines())

            # Se elimina la información descargada
            os.system('rm Descargas/*.csv')

            # CSV Linaje.Schedules
            voArchivo.nbr_id_ejec = nbr_Id_Ejec_Actual
            voArchivo.str_id_archivo = os.path.basename(objRita.str_ArchivoDescargado + '.csv')
            voArchivo.nbr_tamanio_archivo = nbr_Tamanio
            voArchivo.nbr_num_registros = nbr_Filas

            # Se filtra el diccionario para traer solo campos de activacion
            dict_Filtrado = {k: v for k, v in objRita.dict_Campos_Schedule.items() if v['Flag'] == 'A'}
            voArchivo.nbr_num_columnas = len(dict_Filtrado)

            voArchivo.str_anio = str(anio)
            voArchivo.str_mes = str(mes)
            voArchivo.str_NombreDataFrame = 'Linaje/Schedules/' + str(anio) + str(mes) + '.csv'
            voArchivo.str_ruta_almac_s3 = str_RutaS3
            voArchivo.crearCSV()

            # CSV Linaje.Archivos_Det
            # Obtenemos los nombres de columnas del diccionario
            # y los ponemos en un arreglo
            voArchivo_Det = voArchivos_Det()
            np_Campos = np.empty([0, 2])
            for key, value in objRita.dict_Campos_Schedule.items():

                # Se pregunta si el campo esta marcado para activarse
                if value['Flag'] == 'A':
                    np_Campos = np.append(np_Campos, [[voArchivo.str_id_archivo, key]], axis=0)

            voArchivo_Det.np_Campos = np_Campos
            voArchivo_Det.str_NombreDataFrame = 'Linaje/SchedulesDet/' + str(anio) + str(mes) + '.csv'
            voArchivo_Det.crearCSV()

        # CSV Linaje.Ejecuciones
        voEjecucion.nbr_id_ejec = nbr_Id_Ejec_Actual
        voEjecucion.str_bucket_s3 = objUtileria.str_NombreBucket
        voEjecucion.str_usuario_ejec = objUtileria.ObtenerUsuario()
        voEjecucion.str_instancia_ejec = objUtileria.ObtenerIp()
        voEjecucion.str_tipo_ejec = 'P'
        voEjecucion.str_url_webscrapping = objRita.str_Url
        voEjecucion.str_status_ejec = 'Ok'
        voEjecucion.dttm_fecha_hora_ejec = datetime.now()
        voEjecucion.str_tag_script =str(subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']))[2:-3]
        voEjecucion.str_NombreDataFrame = 'Linaje/Ejecuciones/' \
                                           + voEjecucion.str_tipo_ejec + '_' \
                                           + str(voEjecucion.nbr_id_ejec) + '.csv'
        voEjecucion.crearCSV()

    print('---Fin web scraping Schedule vuelos---\n')

def Predict():

    import boto3
    import io
    import pickle as pickle

    objUtileria = Utileria()

    str_Dir = 'modelo_seleccionado/'

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=objUtileria.str_NombreBucket, Key=str_Dir+'parametros.pickle')

    file_pickle = io.BytesIO(obj['Body'].read())

    with open("ParametrosModelo.p", "wb") as outfile:
        # Copy the BytesIO stream to the output file
        outfile.write(file_pickle.getbuffer())

    # Carga de parámetros
    pickleFile = open('ParametrosModelo.p', 'rb')
    modelo = pickle.load(pickleFile)
    pickleFile.close()

    import pandas as pd

    from Class_Eda import Eda
    from Class_ValueObjects import voModeling
    from datetime import datetime

    # objUtileria = Utileria()
    voModeling = voModeling()

    # Instanciamos el objeto Eda
    objEda = Eda()

    # Inicializamos los parámetros principales (por el momento, sólo es uno: la ruta de la fuente de datos)
    objEda.strRutaDataSource = 'DatasetModeladoPredict.csv'  # PREDICT

    df_Input = pd.read_csv('DatasetModeladoPredict.csv')

    # Especificamos nuestro separador de columnas y cargamos el dataset
    objEda.strSeparadorColumnas = ','
    objEda.Cargar_Datos()

    # Proceso de limpieza
    objEda.Limpiar_Datos()

    # Eliminamos las columnas
    #objEda.pdDataSet = objEda.pdDataSet.drop(['etiqueta1'], axis=1)

    objEda.pdDataSet = objEda.pdDataSet.drop(['fecha'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['id_operador'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['salida_realf'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['bandera_delay'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['ind_retraso2'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['ind_retraso3'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['sum_efectos_domino'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['tot_sum_domino'], axis=1)

    objEda.pdDataSet = objEda.pdDataSet.drop(['tiempo_trans_vuelo'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['distancia_millas'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['delay2'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['ind_retraso1'], axis=1)

    objEda.pdDataSet = objEda.pdDataSet.drop(['efecto'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['year'], axis=1)

    # Variables a incluir que se eliminan en esta prueba:
    objEda.pdDataSet = objEda.pdDataSet.drop(['horasalidaf'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['hora_llegada_progf'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['num_vuelo'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['id_avion'], axis=1)

    # Hacemos el label encoder para cada columna por separado
    # Esto es para que no se incremente tanto el número de columnas
    # del dataset de golpe y así evitar problemas de memoria
    objEda.npLabelEncoderFeat = np.array([])
    objEda.Agregar_Features_LabelEnc('day_sem')
    objEda.Agregar_Features_LabelEnc('origen')
    objEda.Agregar_Features_LabelEnc('destino')

    objEda.LabelEncoder_OneHotEncoder()
    objEda.Borrar_Cols_Base_LabelEnc()
    objEda.Borrar_Cols_Inter_LabelEnc()

    X = objEda.pdDataSet.to_numpy()
    X = np.nan_to_num(X)

    np_y = modelo.predict(X)
    df_Input['y_hat'] = np_y
    df_Input.to_csv('Predicciones.csv', index=False, header=False)

    return 0
