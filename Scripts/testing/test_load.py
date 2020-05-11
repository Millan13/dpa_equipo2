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

    objUtileria = Utileria()

    # Variables para metadata
    str_NombreMetodo = ''
    str_Estatus = ''
    str_Mensaje = ''
    dt_HoraEjec = None
    str_NombreArchivo = ''

    # Variable reservada
    __name__ = 'TestLoad'

    #def test_pandas_dataframes_equals(self):
    def test_load_count_columns(self):

        # Contar número de columnas de cada csv en S3

        __s3 = boto3.client('s3') 
        __str_note = 'no match columns'

        #for archivo in
        # Modificar Bucket y Key
        __obj = __s3.get_object(Bucket=self.objUtileria.str_NombreBucket, Key='carga_inicial/2016/February/1016151359_T_ONTIME_REPORTING.csv')
        __df = pd.read_csv(io.BytesIO(__obj['Body'].read()))
        __df.drop(__df.filter(regex="Unname"),axis=1, inplace=True)
        __num_columns = len(__df.columns)

        #df_1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        #df_2 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})

        self.str_NombreMetodo = 'test_load_count_columns'
        self.dt_HoraEjec = datetime.now()

        try:
            #pd.testing.assert_frame_equal(df_1, df_2)
            self.assertEqual(__num_columns,15,note=__str_note)
            self.str_Estatus = 'OK'
        except BaseException as errorPrueba:
            self.str_Estatus = 'FAILED'
            self.str_Mensaje = errorPrueba
