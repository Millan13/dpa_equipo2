#!pip install selenium
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
import os

#### Funciones ####
def crearDriverChrome(str_DirDriver, str_DirDescargas):

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

def bajarAnio(anio, arr_meses):

    for mes in arr_Meses:
        print(mes)
        # Bajamos los meses
        bajarAnioMes(anio, mes)

    return 0

def bajarAnioMes(anio,mes):

    #Creacion del driver y conexion a la Url
    driver = crearDriverChrome(str_DirDriver, str_DirDescargas);

    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': str_DirDescargas}}
    command_result = driver.execute("send_command", params)

    str_Url='https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236'
    driver.get(str_Url)

    time.sleep(3)

    # Bajamos el anio y mes indicados
    driver.find_element_by_xpath("//select[@name='XYEAR']/option[text()="+str(anio)+"]").click()
    driver.find_element_by_xpath("//select[@name='FREQUENCY']/option[text()='"+str(mes)+"']").click()
    driver.execute_script('tryDownload()')
    time.sleep(5)

    #Cerramos el driver junto con el browser
    driver.quit()

    return 0

def configurarVariables(str_Ambiente, str_TipoEjecucion):

    if str_Ambiente=='Local':
        str_DirDriver='/Users/Marco/Ciencia_de_Datos/Maestria/2do_Semestre/Liliana/Pruebas/chromedriver.exe'
        str_DirDescargas='/Users/Marco/Ciencia_de_Datos/Maestria/2do_Semestre/Liliana/Pruebas/Descargas'
    elif str_Ambiente=='EC2':
        str_DirDriver='/home/ec2-user/chromedriver.exe'
        str_DirDescargas='/home/ec2-user/Descargas'

    if str_TipoEjecucion=='Prueba':
        arr_Anios=[2016,2017]
        arr_Meses=['January','February']
    elif str_TipoEjecucion=='Real':
        arr_Anios=[2016,2017,2018,2019]
        arr_Meses=['January','February','March','April','May','June'
            ,'July','August','September','October','November','December']

    return str_DirDriver, str_DirDescargas, arr_Anios, arr_Meses

################# Flujo principal #################

print('\n--Inicio del webscraping--')

### Variables para controlar el ambiente y tipo de ejecución
str_Ambiente='Local'
#str_Ambiente='EC2'
str_TipoEjecucion='Prueba'
#str_TipoEjecucion='Real'

#Cargamos los valores que varían de acuerdo al ambiente y tipo de ejecución que etamos corriendo
str_DirDriver, str_DirDescargas, arr_Anios, arr_Meses = configurarVariables(str_Ambiente, str_TipoEjecucion)

### Barremos los anios
for anio in arr_Anios:
    print(anio)
    bajarAnio(anio,arr_Meses)

print('--Fin del webscraping--\n')
