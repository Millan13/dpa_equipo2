import os
import numpy as np
import subprocess
from datetime import datetime
from Auxiliar import Auxiliar


def CrearDB():

    print('\n---Inicio creacion DB ---\n')

    import psycopg2

    objAuxiliar = Auxiliar()

    if objAuxiliar.ExisteBaseCreada():
        print('Ya existe una BD creada')
    else:
        conn = psycopg2.connect(user=objAuxiliar.str_UsuarioDB,
                                host=objAuxiliar.str_EndPointDB,
                                password=objAuxiliar.str_PassDB)

        conn.autocommit = True
        queries = objAuxiliar.ObtenerQueries()
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
                       'Linaje/ArchivosDet']

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
    from Auxiliar import Auxiliar
    from Rita import RitaWebScraping

    objAuxiliar = Auxiliar()
    objWebScraping = RitaWebScraping()

    arr_Anios = objWebScraping.ObtenerAnios()
    arr_Meses = objWebScraping.ObtenerMeses()

    cnx_S3 = objAuxiliar.CrearConexionS3()
    for anio in arr_Anios:
        print('anio: ', anio)
        for mes in arr_Meses:
            print('mes: ', mes)
            directory_name = 'carga_inicial/' + str(anio) + '/' + str(mes)
            print('directory_name: ', directory_name)

            try:
                cnx_S3.put_object(Bucket=objAuxiliar.str_NombreBucket, Key=(directory_name+'/'))
            except Exception:
                print('Excepcion en CrearDirectoriosS3-put_object():')
                raise
                return 1

    directory_name = 'carga_recurrente'
    print('directory_name: ', directory_name)
    try:
        cnx_S3.put_object(Bucket=objAuxiliar.str_NombreBucket, Key=(directory_name + '/'))
    except Exception:
        print('Excepcion en CrearDirectoriosS3-put_object():')
        raise
        return 1
    print('---Fin creacion directorio S3---\n')
    return 0


def CrearSchemasRDS():
    print('---Inicio creacion schemas---\n')
    objAuxiliar = Auxiliar()
    conn = objAuxiliar.CrearConexionRDS()
    conn.autocommit = True
    queries = objAuxiliar.ObtenerQueries()
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


def CrearTablasLinajeRDS():
    print('---Inicio creacion tablas---\n')
    objAuxiliar = Auxiliar()
    conn = objAuxiliar.CrearConexionRDS()
    conn.autocommit = True
    queries = objAuxiliar.ObtenerQueries()
    query = queries.get('create_linaje_tables')

    try:
        with conn.cursor() as (cur):
            cur.execute(query)
    except Exception:
        print('Excepcion en CrearTablasLinajeRDS-cur.execute')
        raise
        return 1

    print('---Fin creacion tablas---\n')
    return 0


def WebScrapingInicial():

    print('\n---Inicio web scraping Inicial---')
    # import glob, os, time
    from Rita import RitaWebScraping
    from Auxiliar import Auxiliar
    from Linaje import voEjecucion
    from Linaje import voArchivos
    from Linaje import voArchivos_Det

    objAuxiliar = Auxiliar()
    objWebScraping = RitaWebScraping()
    arr_Anios = objWebScraping.ObtenerAnios()
    arr_Meses = objWebScraping.ObtenerMeses()

    objEjecucion = voEjecucion()
    objArchivo = voArchivos()

    # Se obtiene el id de ejecución
    nbr_Id_Ejec_Actual = objAuxiliar.ObtenerMaxId() + 1

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
                cnx_S3 = objAuxiliar.CrearConexionS3()
                str_ArchivoLocal = 'Descargas/' + os.path.basename(objWebScraping.str_ArchivoDescargado + '.csv')
                str_RutaS3 = 'carga_inicial/' + str(anio) + '/' + mes + '/'

                try:
                    objAuxiliar.MandarArchivoS3(cnx_S3, objAuxiliar.str_NombreBucket, str_RutaS3, str_ArchivoLocal)
                except Exception:
                    print('Excepcion en MandarArchivoS3')
                    raise
                    return 1

                # Antes de eliminar los archivos que ya fueron enviados a S3,
                # obtenemos información de ellos
                nbr_Tamanio = objAuxiliar.ObtenerTamanioArchivo(objWebScraping.str_ArchivoDescargado + '.csv')
                nbr_Filas = len(open(objWebScraping.str_ArchivoDescargado + '.csv').readlines())

                # Se elimina la información descargada
                os.system('rm Descargas/*.csv')

                # CSV Linaje.Archivos
                objArchivo.nbr_id_ejec = nbr_Id_Ejec_Actual
                objArchivo.str_id_archivo = os.path.basename(objWebScraping.str_ArchivoDescargado + '.csv')
                objArchivo.nbr_tamanio_archivo = nbr_Tamanio
                objArchivo.nbr_num_registros = nbr_Filas

                # Se filtra el diccionario para traer solo campos de activacion
                dict_Filtrado = {k: v for k, v in objWebScraping.dict_Campos.items() if v['Flag'] == 'A'}
                objArchivo.nbr_num_columnas = len(dict_Filtrado)

                objArchivo.str_anio = str(anio)
                objArchivo.str_mes = str(mes)
                objArchivo.str_NombreDataFrame = 'Linaje/Archivos/' + str(anio) + str(mes) + '.csv'
                objArchivo.str_ruta_almac_s3 = str_RutaS3
                objArchivo.crearCSV()

                # CSV Linaje.Archivos_Det
                # Obtenemos los nombres de columnas del diccionario
                # y los ponemos en un arreglo
                objArchivo_Det = voArchivos_Det()
                np_Campos = np.empty([0, 2])
                for key, value in objWebScraping.dict_Campos.items():

                    # Se pregunta si el campo esta marcado para activarse
                    if value['Flag'] == 'A':
                        np_Campos = np.append(np_Campos, [[objArchivo.str_id_archivo, key]], axis=0)

                objArchivo_Det.np_Campos = np_Campos
                objArchivo_Det.str_NombreDataFrame = 'Linaje/ArchivosDet/' + str(anio) + str(mes) + '.csv'
                objArchivo_Det.crearCSV()

    # CSV Linaje.Ejecuciones
    objEjecucion.nbr_id_ejec = nbr_Id_Ejec_Actual
    objEjecucion.str_bucket_s3 = objAuxiliar.str_NombreBucket
    objEjecucion.str_usuario_ejec = objAuxiliar.ObtenerUsuario()
    objEjecucion.str_instancia_ejec = objAuxiliar.ObtenerIp()
    objEjecucion.str_tipo_ejec = 'CI'
    objEjecucion.str_url_webscrapping = objWebScraping.str_Url
    objEjecucion.str_status_ejec = 'Ok'
    objEjecucion.dttm_fecha_hora_ejec = datetime.now()
    objEjecucion.str_tag_script =str(subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']))[2:-3]
    objEjecucion.str_NombreDataFrame = 'Linaje/Ejecuciones/' \
                                       + objEjecucion.str_tipo_ejec + '_' \
                                       + str(objEjecucion.nbr_id_ejec) + '.csv'
    objEjecucion.crearCSV()

    print('---Fin web scraping Inicial---\n')
    return 0


def EnviarMetadataLinajeRDS():
    print('\n---Inicio carga de linaje---\n')
    from pathlib import Path
    objAuxiliar = Auxiliar()
    cnn = objAuxiliar.CrearConexionRDS()
    cnn.autocommit = True

    # Barremos los csv de Ejecuciones
    for data_file in Path('Linaje/Ejecuciones').glob('*.csv'):

        try:
            objAuxiliar.InsertarEnRDSDesdeArchivo(cnn, data_file, 'ejecuciones')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-ejecuciones
    os.system('rm Linaje/Ejecuciones/*.csv')

    # Barremos los csv de Archivos
    for data_file in Path('Linaje/Archivos').glob('*.csv'):

        try:
            objAuxiliar.InsertarEnRDSDesdeArchivo(cnn, data_file, 'archivos')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-archivos
    os.system('rm Linaje/Archivos/*.csv')

    # Barremos los csv de ArchivosDet
    for data_file in Path('Linaje/ArchivosDet').glob('*.csv'):

        try:
            objAuxiliar.InsertarEnRDSDesdeArchivo(cnn, data_file, 'archivos_det')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeRDS')
            raise
            return 1

    # Eliminamos el arhivo de linaje-archivosdet
    os.system('rm Linaje/ArchivosDet/*.csv')

    print('\n---Fin carga de linaje---\n')
    return 0


def WebScrapingRecurrente():

    print('\n---Inicio web scraping recurrente---')
    # import glob, os, time
    from Rita import RitaWebScraping
    from Auxiliar import Auxiliar
    from Linaje import voEjecucion
    from Linaje import voArchivos
    from Linaje import voArchivos_Det

    objAuxiliar = Auxiliar()
    objWebScraping = RitaWebScraping()
    arr_Anios = objWebScraping.ObtenerAnios()
    arr_Meses = objWebScraping.ObtenerMeses()

    objEjecucion = voEjecucion()
    objArchivo = voArchivos()

    # Se obtiene el id de ejecución
    nbr_Id_Ejec_Actual = objAuxiliar.ObtenerMaxId() + 1

    #Extraemos el último mes y año disponibles para descarga
    latest = objWebScraping.ObtenerMesDescargaRecurrente()
    latest_date = latest.split(" ") #Separamos el mes y el año por espacio
    anio =  latest_date[1]
    mes  =  latest_date[0]
    print('anio: ', anio)
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
        cnx_S3 = objAuxiliar.CrearConexionS3()
        str_ArchivoLocal = 'Descargas/' + os.path.basename(objWebScraping.str_ArchivoDescargado + '.csv')
        str_RutaS3 = 'carga_recurrente/' + str(anio) + '/' + mes + '/'

        try:
            objAuxiliar.MandarArchivoS3(cnx_S3, objAuxiliar.str_NombreBucket, str_RutaS3, str_ArchivoLocal)
        except Exception:
            print('Excepcion en MandarArchivoS3')
            raise
            return 1

        # Antes de eliminar los archivos que ya fueron enviados a S3,
        # obtenemos información de ellos
        nbr_Tamanio = objAuxiliar.ObtenerTamanioArchivo(objWebScraping.str_ArchivoDescargado + '.csv')
        nbr_Filas = len(open(objWebScraping.str_ArchivoDescargado + '.csv').readlines())

        # Se elimina la información descargada
        os.system('rm Descargas/*.csv')

        # CSV Linaje.Archivos
        objArchivo.nbr_id_ejec = nbr_Id_Ejec_Actual
        objArchivo.str_id_archivo = os.path.basename(objWebScraping.str_ArchivoDescargado + '.csv')
        objArchivo.nbr_tamanio_archivo = nbr_Tamanio
        objArchivo.nbr_num_registros = nbr_Filas

        # Se filtra el diccionario para traer solo campos de activacion
        dict_Filtrado = {k: v for k, v in objWebScraping.dict_Campos.items() if v['Flag'] == 'A'}
        objArchivo.nbr_num_columnas = len(dict_Filtrado)

        objArchivo.str_anio = str(anio)
        objArchivo.str_mes = str(mes)
        objArchivo.str_NombreDataFrame = 'Linaje/Archivos/' + str(anio) + str(mes) + '.csv'
        objArchivo.str_ruta_almac_s3 = str_RutaS3
        objArchivo.crearCSV()

        # CSV Linaje.Archivos_Det
        # Obtenemos los nombres de columnas del diccionario
        # y los ponemos en un arreglo
        objArchivo_Det = voArchivos_Det()
        np_Campos = np.empty([0, 2])
        for key, value in objWebScraping.dict_Campos.items():

            # Se pregunta si el campo esta marcado para activarse
              if value['Flag'] == 'A':
                np_Campos = np.append(np_Campos, [[objArchivo.str_id_archivo, key]], axis=0)

        objArchivo_Det.np_Campos = np_Campos
        objArchivo_Det.str_NombreDataFrame = 'Linaje/ArchivosDet/' + str(anio) + str(mes) + '.csv'
        objArchivo_Det.crearCSV()

    # CSV Linaje.Ejecuciones
    objEjecucion.nbr_id_ejec = nbr_Id_Ejec_Actual
    objEjecucion.str_bucket_s3 = objAuxiliar.str_NombreBucket
    objEjecucion.str_usuario_ejec = objAuxiliar.ObtenerUsuario()
    objEjecucion.str_instancia_ejec = objAuxiliar.ObtenerIp()
    objEjecucion.str_tipo_ejec = 'R'
    objEjecucion.str_url_webscrapping = objWebScraping.str_Url
    objEjecucion.str_status_ejec = 'Ok'
    objEjecucion.dttm_fecha_hora_ejec = datetime.now()
    objEjecucion.str_tag_script =str(subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']))[2:-3]
    objEjecucion.str_NombreDataFrame = 'Linaje/Ejecuciones/' \
                                       + objEjecucion.str_tipo_ejec + '_' \
                                       + str(objEjecucion.nbr_id_ejec) + '.csv'
    objEjecucion.crearCSV()

    print('---Fin web scraping recurrente---\n')
    return 0
