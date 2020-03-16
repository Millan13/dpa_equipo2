# Importamos la clase
import glob
import os
import time
from RitaWebScrap import RitaWebScraping
from RitaWebScrap import Auxiliar

print('\n---Inicio web scraping Inicial---')

# Instanciamos la clase Auxiliar
objAuxiliar = Auxiliar()
arr_Anios=objAuxiliar.ObtenerAnios()
arr_Meses=objAuxiliar.ObtenerMeses()

# Instanciamos la clase que realizará el WebScraping
objWebScraping = RitaWebScraping()

# Asignamos la propiedades del ambiente donde se trabajará
for anio in arr_Anios:
    print('anio: ', anio)
    for mes in arr_Meses:
        print('mes: ', mes)

        objWebScraping.DescargarAnioMes(anio,mes);
        str_name=objWebScraping.str_ArchivoDescargado

        # En caso de que sí se haya podido descargar el archivo
        if objWebScraping.str_ArchivoDescargado != '':

            print('Descarga completa')

            # Proceso de descomprimir y borrar zipeado
            print('objWebScraping.str_ArchivoDescargado: ', objWebScraping.str_ArchivoDescargado)
            os.system("unzip 'Descargas/*.zip' -d Descargas/")
            os.system("rm Descargas/*.zip")

            cnx_S3 = objAuxiliar.CrearConexionS3()
            bucket_name = "bucket-rita"
            str_ArchivoLocal='Descargas/'+os.path.basename(objWebScraping.str_ArchivoDescargado+'.csv')
            str_RutaS3='carga_inicial/'+str(anio)+'/'+mes+'/'
            # objWebScraping.MandarArchivoS3(cnx_S3, bucket_name, str_RutaS3, str_ArchivoLocal)

            # Una vez mandado el archivo a S3, lo borramos de la carpeta de Descargas
            # os.system("rm Descargas/*.csv")

print('---Fin web scraping Inicial---\n')
