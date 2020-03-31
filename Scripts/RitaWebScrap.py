#!pip install selenium
from selenium import webdriver
import time
import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
import os
import numpy as np
import boto3
import glob

class RitaWebScraping:

    #Declaración de propiedades
    #arr_Anios=[]
    #arr_Meses=[]
    arr_Campos=[]

    str_Ambiente=''
    str_Url=''
    
    
    # Campos deseados
    dict_campos_activar = {
                          'Year':3
                          ,'Month':5
                          ,'DayofMonth':6
                          ,'DayofWeek':7
                          ,'DepTime':37
                          ,'CRSDepTime':36
                          #,'CRSArrTime':48
                          #,'Reporting_Airline':10
                          #,'Flight_Number_Reporting_Airline':14
                          #,'Tail_Number':13
                          #,'CRSElapsedTime':60
                          #,'DepDelayMinutes':38
                          #,'Origin':19
                          #,'Dest':29
                          #,'Distance':64
                          }
    # Campos pre-seleccionados
    dict_campos_desactivar = {'OriginAirportID':16
                             ,'OriginAirportSeqID':17
                             ,'OriginCityMarketID':18
                             ,'DestAirportID':26
                             ,'DestAirportSeqID':27
                             ,'DestCityMarketID':28}


    #Directorios
    str_DirDriver=''
    str_DirDescargas=''
    str_ArchivoDescargado=''

    #Declaración de métodos
    def __init__(self):

        # Cargamos los directorios de trabajo
        self.str_DirDriver, self.str_DirDescargas = Auxiliar().ObtenerDirectorios()
        self.str_Url='https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236'

        return

    def DescargarAnioMes(self, nbr_Anio, str_Mes):

        #Creacion del driver y conexion a la Url
        driver = self.CrearDriverChrome(self.str_DirDriver, self.str_DirDescargas);

        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': self.str_DirDescargas}}
        command_result = driver.execute("send_command", params)

        driver.get(self.str_Url)

        nbr_Aleat=np.random.uniform(0, 1, 1)
        time.sleep(nbr_Aleat)

        # Bajamos el anio y mes indicados
        driver.find_element_by_xpath("//select[@name='XYEAR']/option[text()="+str(nbr_Anio)+"]").click()
        driver.find_element_by_xpath("//select[@name='FREQUENCY']/option[text()='"+str(str_Mes)+"']").click()

        # Seleccionamos los campos deseados
        
        # Seleccionamos los campos que están pre-seleccionados al abrir la página de donde se hará la descarga de la información
        #for campo in self.dict_campos_desactivar.values():
        #    xpath_preselec = "/html/body/div[3]/div[3]/table[1]/tbody/tr/td[2]/table[4]/tbody/tr[%d]/td[1]/input[@type=\'checkbox\']"% campo
        #    driver.find_element_by_xpath(xpath_preselec).click()
        #time.sleep(5)
         
        # Seleccionamos los campos deseados para crear la base de datos
        for campo in self.dict_campos_activar.values():
            xpath_finales = "/html/body/div[3]/div[3]/table[1]/tbody/tr/td[2]/table[4]/tbody/tr[%d]/td[1]/input[@type=\'checkbox\']"% campo
            driver.find_element_by_xpath(xpath_finales).click()



        
        
        # Bajamos el archivo
        driver.execute_script('tryDownload()')
        str_ext=''

        # Este while es para esperar a que termine la descarga completa del archivo en turno
        while str_ext != '.zip':
            print(datetime.datetime.now())
            list_file = glob.glob(self.str_DirDescargas+'/*.zip') # * means all if need specific format then *.csv
            if len(list_file) != 0:
                str_file=list_file[0]
                arr_aux = os.path.splitext(str_file)
                str_name=arr_aux[0]
                str_ext=arr_aux[1]
            time.sleep(1)

        # Guardamos en la clase el nombre del archivo
        self.str_ArchivoDescargado=str_name

        #Cerramos el driver junto con el browser
        driver.quit()

        return 0

    def CrearDriverChrome(self, str_DirDriver, str_DirDescargas):

        #Preparamos el directorio correspondiente
        PROJECT_ROOT = os.path.abspath(os.path.dirname(str_DirDriver))
        DRIVER_BIN = os.path.join(PROJECT_ROOT, "chromedriver")

        ### Config Chrome
        chrome_options = webdriver.ChromeOptions()
        prefs = { 'download.default_directory': str_DirDescargas
                , 'download.prompt_for_download': False
                , 'download.directory_upgrade': True
                , 'safebrowsing_for_trusted_sources_enabled': False
                , 'safebrowding.enabled': False
                , 'profile.default_content_setting_values.automatic_downloads': 1
                , 'profile.default_content_settings.popups': 0
                }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')

        return webdriver.Chrome(executable_path = DRIVER_BIN, options=chrome_options)

    def MandarArchivoS3(self, cnx_S3, bucket_name, str_RutaS3, str_Archivo):

        str_ArchivoEnvio=str_Archivo
        str_NombreArchivoEnS3 = str_RutaS3+os.path.basename(str_Archivo)

        # Mandamos el archivo a S3
        print('str_ArchivoEnvio: ', str_ArchivoEnvio)
        print('str_NombreArchivoEnS3: ', str_NombreArchivoEnS3)

        print('Enviando el archivo a S3')
        cnx_S3.upload_file(str_ArchivoEnvio, bucket_name, str_NombreArchivoEnS3)

        return

class Auxiliar:

    #str_Ambiente='Local'
    str_Ambiente='EC2'

    str_TipoEjecucion='Prueba'
    #str_TipoEjecucion='Real'

    arr_Anios=[]
    arr_Meses=[]

    def ObtenerAnios(self):

        if self.str_TipoEjecucion == 'Prueba':
            self.arr_Anios=[2016]
        else:
            self.arr_Anios=[2016,2017,2018,2019]

        return self.arr_Anios

    def ObtenerMeses(self):

        if self.str_TipoEjecucion == 'Prueba':
            self.arr_Meses=['January']
        else:
            self.arr_Meses=['January','February','March','April','May','June'
            ,'July','August','September','October','November','December']

        return self.arr_Meses

    def ObtenerDirectorios(self):

        if self.str_Ambiente=='Local':
            self.str_DirDriver='/Users/Marco/Ciencia_de_Datos/Maestria/2do_Semestre/Liliana/Pruebas/chromedriver.exe'
            self.str_DirDescargas='/Users/Marco/Ciencia_de_Datos/Maestria/2do_Semestre/Liliana/Pruebas/Descargas'
        elif self.str_Ambiente=='EC2':
            self.str_DirDriver='/home/ec2-user/dpa_equipo2/Scripts/chromedriver.exe'
            self.str_DirDescargas='/home/ec2-user/dpa_equipo2/Scripts/Descargas'

        return self.str_DirDriver, self.str_DirDescargas

    def ObtenerDirectorioTrabajo(self):

        if self.str_Ambiente=='Local':
            self.str_DirTrabajo='/Users/Marco/Ciencia_de_Datos/Maestria/2do_Semestre/Liliana/Pruebas/'
        elif self.str_Ambiente=='EC2':
            self.str_DirTrabajo='/home/ec2-user/dpa_equipo2/Scripts'

        return self.str_DirTrabajo

    def CrearConexionS3(self):

        from boto3 import Session
        session = Session()

        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()

        s3=boto3.client(
                's3',
                aws_access_key_id=current_credentials.access_key[1:-1],
                aws_secret_access_key=current_credentials.secret_key[1:-1],
                region_name='us-west-2', #Oregon
                use_ssl=False
            )

        return s3
