
import sys
import pandas as pd
import marbles.core
import boto3
import io
#import unittest
from datetime import datetime


#class TestPandas(unittest.TestCase):
class TestLoad(marbles.core.TestCase):

    from Class_Utileria import Utileria
    from Class_Rita import Rita

    
    objRita = Rita()
    objUtileria = Utileria()

    # Variables para metadata
    str_NombreMetodo = ''
    str_Estatus = ''
    str_Mensaje = ''
    dt_HoraEjec = None
    str_NombreArchivo = ''

    # Variable reservada
    __name__ = 'TestLoad'


    def test_load_count_columns(self):


        

        # Contar número de columnas de cada csv en S3

        __s3 = boto3.client('s3')
        __s3_resource = boto3.resource('s3')
        __bucket = __s3_resource.Bucket(self.objUtileria.str_NombreBucket)
        __str_note = 'numero de columnas en el archivo csv no es el esperado'

        __arr_Anios = self.objRita.ObtenerAnios()
        __arr_Meses = self.objRita.ObtenerMeses()

        


       	for anio in __arr_Anios:
            for mes in __arr_Meses:

                __aniio = str(anio)
                __mess = str(mes)

                print(__aniio)
                print(__mess)

                __conn = self.objUtileria.CrearConexionRDS()
                __query ="select id_archivo from linaje.archivos where anio='"+__aniio+"' and mes='"+__mess+"';"
                __cur = __conn.cursor()
                __cur.execute(__query)
                __nombre_archivo = __cur.fetchone()[0]
                print(__nombre_archivo)

                __str_directorio = 'carga_inicial/' + str(anio) + '/' + str(mes) + '/' + __nombre_archivo



                __obj = __s3.get_object(Bucket=self.objUtileria.str_NombreBucket, Key=__str_directorio)
                __df = pd.read_csv(io.BytesIO(__obj['Body'].read()))
                __df.drop(__df.filter(regex="Unname"),axis=1, inplace=True)
                __num_columns = len(__df.columns)


                self.str_NombreMetodo = 'test_load_count_columns'
                self.dt_HoraEjec = datetime.now()

                try:
                    self.assertEqual(__num_columns,15,note=__str_note)
                    self.str_Estatus = 'OK'
                except BaseException as errorPrueba:
                    self.str_Estatus = 'FAILED'
                    self.str_Mensaje = errorPrueba
                    break

