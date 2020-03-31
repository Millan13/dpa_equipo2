# Importamos la clase
import glob
import os
import time
from RitaWebScrap import RitaWebScraping
from RitaWebScrap import Auxiliar
from RitaWebScrap import voEjecucion

print('\n---Inicio web scraping Inicial---')

# Instanciamos la clase Auxiliar
objAuxiliar = Auxiliar()
arr_Anios=objAuxiliar.ObtenerAnios()
arr_Meses=objAuxiliar.ObtenerMeses()

# Instanciamos la clase que realizará el WebScraping
objWebScraping = RitaWebScraping()
objEjecucion = voEjecucion()

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
            objWebScraping.MandarArchivoS3(cnx_S3, bucket_name, str_RutaS3, str_ArchivoLocal)

            # Una vez mandado el archivo a S3, lo borramos de la carpeta de Descargas
            os.system("rm Descargas/*.csv")
            objEjecucion.str_id_archivo=os.path.basename(objWebScraping.str_ArchivoDescargado+'.csv')
            objEjecucion.str_bucket_s3=bucket_name
            objEjecucion.str_ruta_almac_s3=str_RutaS3
            objEjecucion.str_tipo_ejec='I'
            objEjecucion.str_NombreDataFrame='Linaje/Ejecuciones/'+str(anio)+str(mes)+'.csv'
            objEjecucion.crearCSV()

print('---Fin web scraping Inicial---\n')
