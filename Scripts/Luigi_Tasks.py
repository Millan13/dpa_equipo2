import os
import numpy as np
from Auxiliar import Auxiliar


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
    bucket_name = 'bucket-rita'
    for anio in arr_Anios:
        print('anio: ', anio)
        for mes in arr_Meses:
            print('mes: ', mes)
            directory_name = 'carga_inicial/' + str(anio) + '/' + str(mes)
            print('directory_name: ', directory_name)

            try:
                cnx_S3.put_object(Bucket=bucket_name, Key=(directory_name+'/'))
            except Exception:
                print('Excepcion en CrearDirectoriosS3-put_object():')
                raise
                return 1

    directory_name = 'carga_recurrente'
    print('directory_name: ', directory_name)
    try:
        cnx_S3.put_object(Bucket=bucket_name, Key=(directory_name + '/'))
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
                bucket_name = 'bucket-rita'
                str_ArchivoLocal = 'Descargas/' + os.path.basename(objWebScraping.str_ArchivoDescargado + '.csv')
                str_RutaS3 = 'carga_inicial/' + str(anio) + '/' + mes + '/'

                try:
                    objAuxiliar.MandarArchivoS3(cnx_S3, bucket_name, str_RutaS3, str_ArchivoLocal)
                except Exception:
                    print('Excepcion en MandarArchivoS3')
                    raise
                    return 1

                objArchivo.nbr_tamanio_archivo = objAuxiliar.ObtenerTamanioArchivo(objWebScraping.str_ArchivoDescargado + '.csv')
                objArchivo.nbr_num_registros = len(open(objWebScraping.str_ArchivoDescargado + '.csv').readlines())
                os.system('rm Descargas/*.csv')

                objEjecucion.str_id_ejec = int(objAuxiliar.ObtenerMaxId() + 1)
                objEjecucion.str_id_archivo = os.path.basename(objWebScraping.str_ArchivoDescargado + '.csv')
                objEjecucion.str_bucket_s3 = bucket_name
                objEjecucion.str_ruta_almac_s3 = str_RutaS3
                objEjecucion.str_usuario_ejec = objAuxiliar.ObtenerUsuario()
                objEjecucion.str_instancia_ejec = objAuxiliar.ObtenerIp()
                objEjecucion.str_NombreDataFrame = 'Linaje/Ejecuciones/' + str(anio) + str(mes) + '.csv'
                objEjecucion.str_tipo_ejec = 'CI'
                objEjecucion.str_url_webscrapping = objWebScraping.str_Url
                objEjecucion.str_status_ejec = 'Ok'
                objEjecucion.crearCSV()

                objArchivo.str_id_archivo = objEjecucion.str_id_archivo
                objArchivo.nbr_num_columnas = len(objWebScraping.dict_campos_activar)
                objArchivo.str_anio = str(anio)
                objArchivo.str_mes = str(mes)
                objArchivo.str_NombreDataFrame = 'Linaje/Archivos/' + str(anio) + str(mes) + '.csv'
                objArchivo.crearCSV()

                # Obtenemos todos los nombres de columnas del diccionario y los ponemos en un arreglo
                objArchivo_Det = voArchivos_Det()
                np_Campos = np.empty([0, 2])
                for campo in objWebScraping.dict_campos_activar:
                    np_Campos = np.append(np_Campos, [[objEjecucion.str_id_archivo, campo]], axis=0)

                objArchivo_Det.np_Campos = np_Campos
                objArchivo_Det.str_NombreDataFrame = 'Linaje/ArchivosDet/' + str(anio) + str(mes) + '.csv'
                objArchivo_Det.crearCSV()

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

    # Barremos los csv de Archivos
    for data_file in Path('Linaje/Archivos').glob('*.csv'):

        try:
            objAuxiliar.InsertarEnRDSDesdeArchivo(cnn, data_file, 'archivos')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeRDS')
            raise
            return 1

    # Barremos los csv de ArchivosDet
    for data_file in Path('Linaje/ArchivosDet').glob('*.csv'):

        try:
            objAuxiliar.InsertarEnRDSDesdeArchivo(cnn, data_file, 'archivos_det')
        except Exception:
            print('Excepcion en EnviarMetadataLinajeRDS')
            raise
            return 1

    print('\n---Fin carga de linaje---\n')
    return 0
