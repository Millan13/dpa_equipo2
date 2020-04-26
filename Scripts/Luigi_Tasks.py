import os
import numpy as np
import subprocess
from datetime import datetime

# Librerias de nosotros
from Utileria import Utileria

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
    from Rita import Rita

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
    from Rita import Rita
    from Linaje import voEjecucion
    from Linaje import voArchivos
    from Linaje import voArchivos_Det
    from pathlib import Path
    import platform

    objUtileria = Utileria()
    objWebScraping = Rita()
    arr_Anios = objWebScraping.ObtenerAnios()
    arr_Meses = objWebScraping.ObtenerMeses()

    objEjecucion = voEjecucion()
    objArchivo = voArchivos()

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
                #print('Se omite el envio a RDS')
                data_file = open(str_ArchivoLocal, "r")
                objUtileria.InsertarEnRDSDesdeArchivo2(cnn, data_file, 'raw.vuelos')

                # Antes de eliminar los archivos que ya fueron enviados a S3,
                # obtenemos información de ellos
                nbr_Tamanio = objUtileria.ObtenerTamanioArchivo(objWebScraping.str_ArchivoDescargado + '.csv')
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
    objEjecucion.str_bucket_s3 = objUtileria.str_NombreBucket
    objEjecucion.str_usuario_ejec = objUtileria.ObtenerUsuario()
    objEjecucion.str_instancia_ejec = objUtileria.ObtenerIp()
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
    from Rita import Rita
    from Linaje import voEjecucion
    from Linaje import voArchivos
    from Linaje import voArchivos_Det

    objUtileria = Utileria()
    objRita = Rita()
    arr_Anios = objRita.ObtenerAnios()
    arr_Meses = objRita.ObtenerMeses()

    objEjecucion = voEjecucion()
    objArchivo = voArchivos()

    # Se obtiene el id de ejecución
    conn = objUtileria.CrearConexionRDS()
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

    try:
        objRita.DescargarAnioMes(anio, mes)
    except Exception:
        print('Excepcion en WebScrapingInicial-DescargarAnioMes')
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
            print('Se omite el envío')
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
        objArchivo.nbr_id_ejec = nbr_Id_Ejec_Actual
        objArchivo.str_id_archivo = os.path.basename(objRita.str_ArchivoDescargado + '.csv')
        objArchivo.nbr_tamanio_archivo = nbr_Tamanio
        objArchivo.nbr_num_registros = nbr_Filas

        # Se filtra el diccionario para traer solo campos de activacion
        dict_Filtrado = {k: v for k, v in objRita.dict_Campos.items() if v['Flag'] == 'A'}
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
        for key, value in objRita.dict_Campos.items():

            # Se pregunta si el campo esta marcado para activarse
              if value['Flag'] == 'A':
                np_Campos = np.append(np_Campos, [[objArchivo.str_id_archivo, key]], axis=0)

        objArchivo_Det.np_Campos = np_Campos
        objArchivo_Det.str_NombreDataFrame = 'Linaje/ArchivosDet/' + str(anio) + str(mes) + '.csv'
        objArchivo_Det.crearCSV()

    # CSV Linaje.Ejecuciones
    objEjecucion.nbr_id_ejec = nbr_Id_Ejec_Actual
    objEjecucion.str_bucket_s3 = objUtileria.str_NombreBucket
    objEjecucion.str_usuario_ejec = objUtileria.ObtenerUsuario()
    objEjecucion.str_instancia_ejec = objUtileria.ObtenerIp()
    objEjecucion.str_tipo_ejec = 'R'
    objEjecucion.str_url_webscrapping = objRita.str_Url
    objEjecucion.str_status_ejec = 'Ok'
    objEjecucion.dttm_fecha_hora_ejec = datetime.now()
    objEjecucion.str_tag_script =str(subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']))[2:-3]
    objEjecucion.str_NombreDataFrame = 'Linaje/Ejecuciones/' \
                                       + objEjecucion.str_tipo_ejec + '_' \
                                       + str(objEjecucion.nbr_id_ejec) + '.csv'
    objEjecucion.crearCSV()

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
    str_NombreQuery = 'update1'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 1, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Query 1
    str_NombreQuery = 'update2'
    query = queries.get(str_NombreQuery)
    nbr_FilasAfec = objUtileria.EjecutarQuery(conn, query)
    CrearMetadataTrans(nbr_IdSet, 2, str_NombreQuery, nbr_FilasAfec, str_Ruta)

    # Aquí se deben de poner el resto de queries del feature engineering

    print('---Fin de feature engineering---\n')

    return 0


def Modelar():
    print('---Inicio de Modelar ---\n')
    from Class_Eda import Eda
    from Linaje import voModeling

    objUtileria = Utileria()
    objModeling = voModeling()

    # Instanciamos el objeto Eda
    objEda = Eda()

    # Inicializamos los parámetros principales (por el momento, sólo es uno: la ruta de la fuente de datos)
    objEda.strRutaDataSource = 'Transit_modeling.csv'

    # Proceso de carga
    objEda.Cargar_Datos()

    # Proceso de limpieza
    objEda.Limpiar_Datos()

    # Guardamos el arreglo en la nueva columna
    objEda.pdDataSet['y'] = objEda.pdDataSet.apply(lambda x: (x.etiqueta1), axis=1)

    # Eliminamos las columnas
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
    objEda.pdDataSet = objEda.pdDataSet.drop(['delay'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['ind_retraso1'], axis=1)

    objEda.pdDataSet = objEda.pdDataSet.drop(['efecto'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['year'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['etiqueta1'], axis=1)

    # Variables a incluir que se eliminan en esta prueba:
    objEda.pdDataSet = objEda.pdDataSet.drop(['horasalidaf'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['hora_llegada_progf'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['num_vuelo'], axis=1)
    objEda.pdDataSet = objEda.pdDataSet.drop(['id_avion'], axis=1)

    # Label encoder
    objEda.npLabelEncoderFeat = np.array([])
    objEda.Agregar_Features_LabelEnc('day_sem')
    objEda.Agregar_Features_LabelEnc('origen')
    objEda.Agregar_Features_LabelEnc('destino')

    # Mostramos el dataSet Auxiliar para ver que aún no ocurre ningún cambio
    objEda.LabelEncoder_OneHotEncoder()

    objEda.Borrar_Cols_Base_LabelEnc()
    objEda.Borrar_Cols_Inter_LabelEnc()

    # Separamos las features de lo que vamos a predecir
    pdX, pdY = objEda.SepararFeaturesYPred('y')

    # Separamos nuestros datos en entrenamiento y pruebas utilizando la proporción 80-20
    objEda.Generar_Train_Test(pdX, pdY, 0.2)

    # Preparamos las variables que imputaremos
    objEda.listTransform = ['']  # Limpiamos la propiedad de lista de features a imputar
    objEda.Agregar_Features_Transform('median', 'vuelos_afectados')  # no hizo nada porque están como NaN

    # Imputamos sobre el conjunto de entrenamiento y prueba
    objEda.X_train = objEda.Imputar_Features(objEda.X_train)
    objEda.X_test = objEda.Imputar_Features(objEda.X_test)

    # Se crean los hyperparámetros con los que se trabajará
    # Arreglo de diccionarios por modelo (deben ir en el órden a ejecutar)
    npDictHiperParam = np.array([])

    # Parametrización para Árboles
    dictHyperParams = {'max_depth': [4],  # [4,7]
                       'min_samples_split': [4],  # [4,16]
                       'min_samples_leaf': [3],  # [3,7]
                       'max_features': ['sqrt']  # ['sqrt','log2']
                       }
    npDictHiperParam = np.append(npDictHiperParam, dictHyperParams)

    # Parametrización para Bosques
    dictHyperParams = {'n_estimators': [25],  # Se redujo a 50
                       'max_depth': [4],  # [4,7]
                       'max_features': ['sqrt'],  # ['sqrt','log2']
                       'min_samples_split': [4],  # [4,16]
                       'min_samples_leaf': [3]  # [3,7]
                       }
    npDictHiperParam = np.append(npDictHiperParam, dictHyperParams)

    # Parametrización para XGBoost
    dictHyperParams = {'learning_rate': [0.25, 0.75],
                       'n_estimators': [25],  # Se redujo a 50
                       'min_samples_split': [4],  # [4,16]
                       'min_samples_leaf': [3],  # [3,7]
                       'max_depth': [4],  # [4,7]
                       'max_features': ['sqrt']
                       }
    npDictHiperParam = np.append(npDictHiperParam, dictHyperParams)

    # Se crean los modelos de clasificaión que se emplearán (en el mismo orden que los diccionarios)
    npNombreModelos = np.array([])
    npNombreModelos = np.append(npNombreModelos, 'DECTREE')
    npNombreModelos = np.append(npNombreModelos, 'RANDOMF')
    npNombreModelos = np.append(npNombreModelos, 'XGBOOST')

    arrModelos = objEda.prepModelos(npNombreModelos)

    # #Se corre el magic loop para realizar las predicciones con los parámetros previamente establecidos
    npGridSearchCv = objEda.magic_loop2(arrModelos,
                                        npDictHiperParam,
                                        objEda.X_train,
                                        objEda.Y_train,
                                        5)

    npArrBestScores = np.array([])
    npArrBestParams = np.array([])

    # Barremos el arreglo de GridSearchCV´s para sacar los mejores scores y parámetros
    for grid in npGridSearchCv:
        npArrBestScores = np.append(npArrBestScores, grid.best_score_)
        npArrBestParams = np.append(npArrBestParams, grid.best_params_)

    # Obtenemos el índice del mejor score
    nbrIndiceGanador = np.argmax(npArrBestScores, axis=0)

    # Mostramos el modelo, parámetros y score ganador
    print("Modelo ganador: \n", arrModelos[nbrIndiceGanador])

    print("Score del modelo ganador: \n", npArrBestScores[nbrIndiceGanador])

    print("Parámetros del modelo ganador: \n", npArrBestParams[nbrIndiceGanador])

    conn = objUtileria.CrearConexionRDS()
    nbr_id_set_modelado = objUtileria.ObtenerMaxId(conn,
                                                   'linaje.modeling',
                                                   'id_set_modelado') + 1
    for grid in npGridSearchCv:
        objModeling.nbr_id_set_modelado = nbr_id_set_modelado
        objModeling.str_nombre_modelo = str(type(grid.estimator))
        objModeling.nbr_mejor_score_modelo = grid.best_score_
        objModeling.str_NombreDataFrame = 'Linaje/Modeling/' \
                                          + objModeling.str_nombre_modelo \
                                          + '.csv'
        objModeling.dttm_fecha_hora_ejec = datetime.now()
        objModeling.str_usuario_ejec = objUtileria.ObtenerUsuario()
        objModeling.str_instancia_ejec = objUtileria.ObtenerIp()
        objModeling.crearCSV()
        # print(grid.param_grid)
        # print(grid.best_params_)

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

# ###############################################################
# #################### Funciones de apoyo #######################
# ###############################################################

def CrearMetadataTrans(nbr_IdSet, nbr_seq, str_NombreQuery, nbr_FilasAfectadas, str_Ruta):

    from Linaje import voTransform
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

    return 0
