
from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
import time
import datetime
import os
import numpy as np
import glob


class Rita:

    # Declaración de propiedades
    str_Url = ''

    str_Ambiente = ''

    str_TipoEjecucion = 'Prueba'
    # str_TipoEjecucion='Real'

    # Campos para activar y desactivar
    dict_Campos = {'Year': {'Id': 3, 'Flag': 'A'},
                   'Month': {'Id': 5, 'Flag': 'A'},
                   'DayofMonth': {'Id': 6, 'Flag': 'A'},
                   'DayofWeek': {'Id': 7, 'Flag': 'A'},
                   'Reporting_Airline': {'Id': 10, 'Flag': 'A'},
                   'Tail_Number': {'Id': 13, 'Flag': 'A'},
                   'Flight_Number_Reporting_Airline': {'Id': 14, 'Flag': 'A'},
                   'OriginAirportID': {'Id': 16, 'Flag': 'I'},
                   'OriginAirportSeqID': {'Id': 17, 'Flag': 'I'},
                   'OriginCityMarketID': {'Id': 18, 'Flag': 'I'},
                   'Origin': {'Id': 19, 'Flag': 'A'},
                   'DestAirportID': {'Id': 26, 'Flag': 'I'},
                   'DestAirportSeqID': {'Id': 27, 'Flag': 'I'},
                   'DestCityMarketID': {'Id': 28, 'Flag': 'I'},
                   'Dest': {'Id': 29, 'Flag': 'A'},
                   'CRSDepTime': {'Id': 36, 'Flag': 'A'},
                   'DepTime': {'Id': 37, 'Flag': 'A'},
                   'DepDelayMinutes': {'Id': 38, 'Flag': 'A'},
                   'CRSArrTime': {'Id': 48, 'Flag': 'A'},
                   'CRSElapsedTime': {'Id': 60, 'Flag': 'A'},
                   'Distance': {'Id': 64, 'Flag': 'A'}
                   }

    # Listas con las estructuras de tablas para el linaje
    lst_Ejecuciones = [("id_ejec", "NUMERIC"),
                       ("usuario_ejec", "VARCHAR"),
                       ("instancia_ejec", "VARCHAR"),
                       ("fecha_hora_ejec", "TIMESTAMP"),
                       ("bucket_s3", "VARCHAR"),
                       ("tag_script", "VARCHAR"),
                       ("tipo_ejec", "VARCHAR"),
                       ("url_webscrapping", "VARCHAR"),
                       ("status_ejec", "VARCHAR")]

    lst_Archivos = [("id_ejec", "NUMERIC"),
                    ("id_archivo", "VARCHAR"),
                    ("num_registros", "VARCHAR"),
                    ("num_columnas", "NUMERIC"),
                    ("tamanio_archivo", "VARCHAR"),
                    ("anio", "VARCHAR"),
                    ("mes", "VARCHAR"),
                    ("ruta_almac_s3", "VARCHAR")]

    lst_ArchivosDet = [("id_archivo", "VARCHAR"),
                       ("nombre_col", "VARCHAR")]

    lst_Transform = [("id_set_transform", "NUMERIC"),
                     ("num_seq", "NUMERIC"),
                     ("nombre_query", "VARCHAR"),
                     ("filas_afectadas", "VARCHAR "),
                     ("fecha_hora_ejec", "TIMESTAMP"),
                     ("usuario_ejec", "VARCHAR"),
                     ("instancia_ejec", "VARCHAR"),
                     ("tipo_ejec", "VARCHAR")]

    lst_Modeling = [("id_set_modelado", "NUMERIC"),
                    ("nombre_modelo", "VARCHAR"),
                    #("hiperparametros", "JSON"),
                    ("mejor_score_modelo", "NUMERIC"),
                    ("fecha_hora_ejec", "TIMESTAMP"),
                    ("usuario_ejec", "VARCHAR"),
                    ("instancia_ejec", "VARCHAR")]

    lst_Predicciones = [("fecha", "VARCHAR"),
                        ("day_sem", "VARCHAR"),
                        ("id_operador", "VARCHAR"),
                        ("id_avion", "VARCHAR"),
                        ("num_vuelo", "VARCHAR"),
                        ("origen", "VARCHAR"),
                        ("destino", "VARCHAR"),
                        ("horasalidaf", "VARCHAR"),
                        ("salida_realf", "VARCHAR"),
                        ("tiempo_trans_vuelo", "VARCHAR"),
                        ("distancia_millas", "VARCHAR"),
                        ("hora_llegada_progf", "VARCHAR"),
                        ("delay2", "VARCHAR"),
                        ("bandera_delay", "VARCHAR"),
                        ("count", "VARCHAR"),
                        ("max", "VARCHAR"),
                        ("nvue_falt", "VARCHAR"),
                        ("ind_retraso1", "VARCHAR"),
                        ("ind_retraso2", "VARCHAR"),
                        ("ind_retraso3", "VARCHAR"),
                        ("efecto", "VARCHAR"),
                        ("sum_efectos_domino", "VARCHAR"),
                        ("tot_sum_domino", "VARCHAR"),
                        ("vuelos_afectados", "VARCHAR"),
                        ("year", "VARCHAR"),
                        ("y_hat", "VARCHAR")]

    # Directorios
    str_DirDriver = ''
    str_DirDescargas = ''
    str_ArchivoDescargado = ''
    str_MasReciente = ''

    # Declaración de métodos
    def __init__(self):

        from Class_Utileria import Utileria

        # Lo primero es: saber en qué ambiente se está trabajando
        self.objUtileria = Utileria()
        if self.objUtileria.ObtenerUsuario() == 'ec2-user':
            self.str_Ambiente = 'EC2'
        else:
            self.str_Ambiente = 'Local'

        # Cargamos los directorios de trabajo
        self.str_DirDriver, self.str_DirDescargas = self.ObtenerDirectorios()
        self.str_Url = 'https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236'

        return

    def DescargarAnioMes(self, nbr_Anio, str_Mes):

        # Creacion del driver y conexion a la Url
        driver = self.CrearDriverChrome(self.str_DirDriver, self.str_DirDescargas)

        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow',
                             'downloadPath': self.str_DirDescargas
                             }
                  }
        command_result = driver.execute("send_command", params)

        print('Esperando a la pagina...', self.str_Url)
        driver.get(self.str_Url)

        # Bajamos el anio y mes indicados
        driver.find_element_by_xpath("//select[@name='XYEAR']/option[text()="+str(nbr_Anio)+"]").click()
        driver.find_element_by_xpath("//select[@name='FREQUENCY']/option[text()='"+str(str_Mes)+"']").click()

        # Seleccionamos los campos deseados para crear la base de datos
        print('Seleccionando campos para descarga...')
        for campo in self.dict_Campos.items():
            xpath_finales = "/html/body/div[3]/div[3]/table[1]/tbody/tr/td[2]/table[4]/tbody/tr[%d]/td[1]/input[@type=\'checkbox\']"% campo[1]['Id']

            element = driver.find_element_by_xpath(xpath_finales)
            driver.execute_script("arguments[0].click();", element)

            # Método 1
            # driver.find_element_by_xpath(xpath_finales).click()

            # Método 2
            # wait = WebDriverWait(driver, 30)
            # element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_finales)))
            # element.click()

            nbr_Aleat = np.random.uniform(1,2,1)
            time.sleep(nbr_Aleat)

        # Bajamos el archivo
        print('Generando archivo...')
        driver.execute_script('tryDownload()')
        str_ext = ''

        # Este while es para esperar a que termine la descarga completa del archivo en turno
        print('Bajando el archivo...')
        while str_ext != '.zip':
            print(datetime.datetime.now())
            list_file = glob.glob(self.str_DirDescargas+'/*.zip')  # * means all if need specific format then *.csv
            if len(list_file) != 0:
                str_file = list_file[0]
                arr_aux = os.path.splitext(str_file)
                str_name = arr_aux[0]
                str_ext = arr_aux[1]
            time.sleep(1)

        # Guardamos en la clase el nombre del archivo
        self.str_ArchivoDescargado = str_name

        # Cerramos el driver junto con el browser
        driver.quit()

        return 0

    def CrearDriverChrome(self, str_DirDriver, str_DirDescargas):

        # Preparamos el directorio correspondiente
        PROJECT_ROOT = os.path.abspath(os.path.dirname(str_DirDriver))
        DRIVER_BIN = os.path.join(PROJECT_ROOT, "chromedriver")

        # ### Config Chrome
        chrome_options = webdriver.ChromeOptions()
        prefs = {'download.default_directory': str_DirDescargas,
                 'download.prompt_for_download': False,
                 'download.directory_upgrade': True,
                 'safebrowsing_for_trusted_sources_enabled': False,
                 'safebrowding.enabled': False,
                 'profile.default_content_setting_values.automatic_downloads': 1,
                 'profile.default_content_settings.popups': 0
                 }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')

        return webdriver.Chrome(executable_path=DRIVER_BIN, options=chrome_options)

    def ObtenerAnios(self):

        if self.str_TipoEjecucion == 'Prueba':
            self.arr_Anios = [2016]
        else:
            self.arr_Anios = [2016, 2017, 2018, 2019]

        return self.arr_Anios

    def ObtenerMeses(self):

        if self.str_TipoEjecucion == 'Prueba':
            self.arr_Meses = ['January', 'February']
        else:
            self.arr_Meses = ['January',
                              'February',
                              'March',
                              'April',
                              'May',
                              'June',
                              'July',
                              'August',
                              'September',
                              'October',
                              'November',
                              'December']

        return self.arr_Meses

    def Modelar(self, str_ArchivoDataSet):

        from Class_Eda import Eda
        from Class_ValueObjects import voModeling
        from datetime import datetime

        #objUtileria = Utileria()
        voModeling = voModeling()

        # Instanciamos el objeto Eda
        objEda = Eda()

        # Inicializamos los parámetros principales (por el momento, sólo es uno: la ruta de la fuente de datos)
        objEda.strRutaDataSource = str_ArchivoDataSet

        # Especificamos nuestro separador de columnas y cargamos el dataset
        objEda.strSeparadorColumnas = ','
        objEda.Cargar_Datos()

        # Temporalmente se trabaja con una porción de los datos
        filas = objEda.pdDataSet.shape[0]
        porcion = int(filas/10)
        objEda.pdDataSet = objEda.pdDataSet.drop(objEda.pdDataSet.index[porcion:filas])
        # filas = objEda.pdDataSet.shape[0]

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
        objEda.pdDataSet = objEda.pdDataSet.drop(['delay2'], axis=1)
        objEda.pdDataSet = objEda.pdDataSet.drop(['ind_retraso1'], axis=1)

        objEda.pdDataSet = objEda.pdDataSet.drop(['efecto'], axis=1)
        objEda.pdDataSet = objEda.pdDataSet.drop(['year'], axis=1)
        objEda.pdDataSet = objEda.pdDataSet.drop(['etiqueta1'], axis=1)

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
                           'max_depth': [5],  # [4,7]
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
        npGridSearchCv = objEda.Correr_Magic_Loop(arrModelos,
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
        # print("Modelo ganador: \n", arrModelos[nbrIndiceGanador])
        # print("Score del modelo ganador: \n", npArrBestScores[nbrIndiceGanador])
        # print("Parametros del modelo ganador: \n", npArrBestParams[nbrIndiceGanador])

        # Se instancia el modelo ganador
        self.ModeloGanadorMagicLoop = objEda.InstanciarModeloDinamico(npNombreModelos, nbrIndiceGanador, npArrBestParams[nbrIndiceGanador])
        self.ModeloGanadorMagicLoop = objEda.best_model

        conn = self.objUtileria.CrearConexionRDS()
        nbr_id_set_modelado = self.objUtileria.ObtenerMaxId(conn,
                                                       'linaje.modeling',
                                                       'id_set_modelado') + 1
        for grid in npGridSearchCv:
            voModeling.nbr_id_set_modelado = nbr_id_set_modelado
            voModeling.str_nombre_modelo = str(type(grid.estimator))
            # voModeling.dict_hiperparametros = str(grid.param_grid)
            voModeling.nbr_mejor_score_modelo = grid.best_score_
            voModeling.str_NombreDataFrame = 'Linaje/Modeling/' \
                                              + voModeling.str_nombre_modelo \
                                              + '.csv'
            voModeling.dttm_fecha_hora_ejec = datetime.now()
            voModeling.str_usuario_ejec = self.objUtileria.ObtenerUsuario()
            voModeling.str_instancia_ejec = self.objUtileria.ObtenerIp()

            # Creamos el CSV que contiene el linaje de esta información
            voModeling.crearCSV()

    def ObtenerDirectorios(self):

        if self.str_Ambiente == 'Local':
            self.str_DirDriver = '/Users/Marco/github/dpa_equipo2/Scripts/chromedriver.exe'
            self.str_DirDescargas = '/Users/Marco/github/dpa_equipo2/Scripts/Descargas'
        elif self.str_Ambiente == 'EC2':
            self.str_DirDriver = '/home/ec2-user/dpa_equipo2/Scripts/chromedriver.exe'
            self.str_DirDescargas = '/home/ec2-user/dpa_equipo2/Scripts/Descargas'

        return self.str_DirDriver, self.str_DirDescargas

    def ObtenerDirectorioTrabajo(self):

        if self.str_Ambiente == 'Local':
            self.str_DirTrabajo = '/Users/Marco/github/dpa_equipo2/Scripts'
        elif self.str_Ambiente == 'EC2':
            self.str_DirTrabajo = '/home/ec2-user/dpa_equipo2/Scripts'

        return self.str_DirTrabajo
    def ObtenerMesDescargaRecurrente(self):
        # Creacion del driver y conexion a la Url
        driver = self.CrearDriverChrome(self.str_DirDriver, self.str_DirDescargas)

        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior',
                  'params': {'behavior': 'allow',
                             'downloadPath': self.str_DirDescargas
                             }
                  }
        command_result = driver.execute("send_command", params)

        driver.get(self.str_Url)

        #Buscamos el mes y anio más reciente disponible en la página
        latest_field = driver.find_element_by_xpath("//table[1]/tbody/tr/td[2]/table[2]/tbody/tr[3]/td[1]")
        self.str_MasReciente =  latest_field.text.replace("Latest Available Data: ","") #Removemos el texto  fijo

        #Cerramos el driver junto con el browser
        driver.quit()

        return self.str_MasReciente
