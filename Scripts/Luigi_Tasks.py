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
                       'Linaje/Modeling']

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
    from pathlib import Path
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
                    # objUtileria.MandarArchivoS3(cnx_S3, objUtileria.str_NombreBucket, str_RutaS3, str_ArchivoLocal)
                    print('Se omite el envio a S3')
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
                archivo = open(str_ArchivoLocal)

                # Mandamos la información raw del archivo al RDS
                print('Se omite el envio a RDS')
                #data_file = open(str_ArchivoLocal, "r")
                #objUtileria.InsertarEnRDSDesdeArchivo2(cnn, data_file, 'raw.vuelos')

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
    print('\n---Inicio envío de linaje carga---\n')
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

    print('\n---Fin envío de linaje carga---\n')
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
    #conn.autocommit = True
    nbr_Id_Ejec_Actual = objUtileria.ObtenerMaxId(conn,
                                                  'linaje.ejecuciones',
                                                  'id_ejec') + 1


    #Extraemos el último mes y año disponibles para descarga
    latest = objRita.ObtenerMesDescargaRecurrente()
    latest_date = latest.split(" ") #Separamos el mes y el año por espacio
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
                # objUtileria.MandarArchivoS3(cnx_S3, objUtileria.str_NombreBucket, str_RutaS3, str_ArchivoLocal)
                print('Se omite el envio')
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


def HacerFeatureEngineering():
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

    # Query 1
    str_NombreQuery = 'Paso0_copytable'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 1, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 2
    str_NombreQuery = 'Paso1_filtroWN'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 2, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 3
    str_NombreQuery = 'Paso2_rename'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 3, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 4
    str_NombreQuery = 'Paso3_delaynumeric'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 4, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 5
    str_NombreQuery = 'Paso4_banderadelay'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 5, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 6
    str_NombreQuery = 'Paso5_crearfecha1'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 6, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 7
    str_NombreQuery = 'Paso6_crearfecha2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 7, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 8
    str_NombreQuery = 'Paso7_crearfecha2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 8, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 9
    str_NombreQuery = 'Paso8_tantitalimpieza'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 9, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 10
    str_NombreQuery = 'Paso9_ordenar'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 10, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 11
    str_NombreQuery = 'Paso10_conteo'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 11, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 12
    str_NombreQuery = 'Paso11_ranking_max'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 12, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 13
    str_NombreQuery = 'Paso12_innerjoin'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 13, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 14
    str_NombreQuery = 'Paso13_vuelosfaltantes'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 14, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 15
    str_NombreQuery = 'Paso14_efectodomino1'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 15, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 16
    str_NombreQuery = 'Paso15_efectodomino2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 16, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 17
    str_NombreQuery = 'Paso16_distraccion'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 17, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 18
    str_NombreQuery = 'Paso17_banderadomino'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 18, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Aquí se deben de poner el resto de queries del feature engineering

    print('---Fin de feature engineering---\n')

    return 0


def Modelar():
    print('---Inicio de Modelar ---\n')

    from Class_Rita import Rita
    import pickle as pickle

    objRita = Rita()

    objRita.Modelar('Transit_modeling.csv')

    # Se guarda el pickle del modelo ganador
    pickleFile = open('parametros.pickle', 'wb')
    pickle.dump(objRita.ModeloGanadorMagicLoop, pickleFile)
    pickleFile.close()

    # Se hace el envío a S3
    EnviarPickleAS3()

    # Aquí se deberá poner la función que envía el pickle a S3
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
    # os.system('rm Linaje/Modeling/*.csv')
    print('\n---Fin envío metadata modeling---\n')
    return 0

# ###############################################################
# #################### Funciones de apoyo #######################
# ###############################################################

def CrearMetadataTrans(nbr_IdSet, nbr_seq, str_NombreQuery, nbr_FilasAfectadas, str_Ruta):

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
    objTransform.str_NombreDataFrame = str_Ruta \
                                     + str(objTransform.nbr_id_set_transform) \
                                     + '_' \
                                     + str(objTransform.nbr_num_seq) + '.csv'

    print(objTransform.nbr_id_set_transform)
    objTransform.crearCSV()


def EnviarPickleAS3():
    objUtileria = Utileria()

    cnx_S3 = objUtileria.CrearConexionS3()
    str_ArchivoPickleLocal = 'parametros.pickle'
    str_RutaS3 = 'modelo_seleccionado/'

    try:
        # objUtileria.MandarArchivoS3(cnx_S3, objUtileria.str_NombreBucket, str_RutaS3, str_ArchivoPickleLocal)
        # print("Pickle enviado a S3")
        print('Se omite el envio')
    except Exception:
        print('Excepcion en EnviarPickleAS3')
        raise
        return 1

    return 0
