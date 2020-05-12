import pandas as pd
import marbles.core
from datetime import datetime
import os

class TestTransform(marbles.core.TestCase):

    # Variables para metadata
    str_NombreMetodo = ''
    str_Estatus = ''
    str_Mensaje = ''
    dt_HoraEjec = None
    str_NombreArchivo = ''

    # Variable reservada
    __name__ = 'TestTransform'

    def test_transform_delay_positive(self):

        __str_RutaScripts = os.path.abspath(os.path.curdir)

        __df = pd.read_csv(__str_RutaScripts + '/DatasetModelado.csv')
        __m = (__df['delay2'] >= 0).all()
        __str_note = 'existen delays negativos'

        self.str_NombreMetodo = 'test_transform_delay_positive'
        self.dt_HoraEjec = datetime.now()

        try:
            self.assertTrue(__m, note=__str_note)
            self.str_Estatus = 'OK'
        except BaseException as errorPrueba:
            self.str_Estatus = 'FAILED'
            self.str_Mensaje = errorPrueba
