import sys
import pandas as pd
import unittest
from datetime import datetime


class TestPandas(unittest.TestCase):

    # Variables para metadata
    str_NombreMetodo = ''
    str_Estatus = ''
    str_Mensaje = ''
    dt_HoraEjec = None
    str_NombreArchivo = ''

    # Variable reservada
    __name__ = 'TestPandas'

    def test_pandas_dataframes_equals(self):

        df_1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        df_2 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})

        self.str_NombreMetodo = 'test_pandas_dataframes_equals'
        self.dt_HoraEjec = datetime.now()

        try:
            pd.testing.assert_frame_equal(df_1, df_2)
            self.str_Estatus = 'OK'
        except BaseException as errorPrueba:
            self.str_Estatus = 'FAILED'
            self.str_Mensaje = errorPrueba
