
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

    # Directorios
    str_DirDriver = ''
    str_DirDescargas = ''
    str_ArchivoDescargado = ''
    str_MasReciente = ''

    # Declaración de métodos
    def __init__(self):

        from Utileria import Utileria

        # Lo primero es: saber en qué ambiente se está trabajando
        objUtileria = Utileria()
        if objUtileria.ObtenerUsuario() == 'ec2-user':
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

        print('Esperando a la página...')
        driver.get(self.str_Url)

        # Bajamos el anio y mes indicados
        driver.find_element_by_xpath("//select[@name='XYEAR']/option[text()="+str(nbr_Anio)+"]").click()
        driver.find_element_by_xpath("//select[@name='FREQUENCY']/option[text()='"+str(str_Mes)+"']").click()

        # Seleccionamos los campos deseados para crear la base de datos
        print('Seleccionando campos para descarga')
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
