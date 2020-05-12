import pandas as pd
import marbles.core
#import io
#import unittest
from datetime import datetime


#class TestPandas(unittest.TestCase):
class TestTransform(marbles.core.TestCase):

    #from Class_Utileria import Utileria

    #objUtileria = Utileria()

    # Variables para metadata
    str_NombreMetodo = ''
    str_Estatus = ''
    str_Mensaje = ''
    dt_HoraEjec = None
    str_NombreArchivo = ''

    # Variable reservada
    __name__ = 'TestTransform'

    #def test_pandas_dataframes_equals(self):
    def test_transform_delay_positive(self):

        __df = pd.read_csv('/home/ec2-user/dpa_equipo2/Scripts/DatasetModelado.csv')
        __df['delay2'].iloc[0] = -1
        __m = (__df['delay2']>=0).all()
        __str_note = 'existen delays negativos'

        self.str_NombreMetodo = 'test_transform_delay_positive'
        self.dt_HoraEjec = datetime.now()

        try:
            #pd.testing.assert_frame_equal(df_1, df_2)
            self.assertTrue(__m,note=__str_note)
            self.str_Estatus = 'OK'
        except BaseException as errorPrueba:
            self.str_Estatus = 'FAILED'
            self.str_Mensaje = errorPrueba

