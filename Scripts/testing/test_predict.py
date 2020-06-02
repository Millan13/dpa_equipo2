import pandas as pd
import marbles.core
from datetime import datetime
import os
from Class_Utileria import Utileria

class TestPredict(marbles.core.TestCase):

    from Class_Utileria import Utileria

    objUtileria = Utileria()
    # Variables para metadata
    str_NombreMetodo = ''
    str_Estatus = ''
    str_Mensaje = ''
    dt_HoraEjec = None
    str_NombreArchivo = ''

    # Variable reservada
    __name__ = 'TestPredict'

    def test_predict_delay_binary(self):
        s = "SELECT *"
        s += " FROM "
        s += "linaje.ejecuciones"

        # Nos conectamos con la RDS.
        __conn = self.objUtileria.CrearConexionRDS()
        __cur = __conn.cursor()

        # Use the COPY function on the SQL we created above.
        SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER;".format(s)

        # Set up a variable to store our file path and name.
        t_path_n_file = "/home/ec2-user/dpa_equipo2/Scripts/Predict.csv"
        with open(t_path_n_file, 'w') as f_output:
          __cur.copy_expert(SQL_for_file_output, f_output)

        __str_RutaScripts = os.path.abspath(os.path.curdir)

        __df = pd.read_csv(__str_RutaScripts + '/Predict.csv')
        __m = __df['id_ejec'].isin([0,1]).all()
        __str_note = 'existen valores distintos de 0 y 1'

        self.str_NombreMetodo = 'test_predict_delay_binary'
        self.dt_HoraEjec = datetime.now()

        try:
            self.assertTrue(__m, note=__str_note)
            self.str_Estatus = 'OK'
        except BaseException as errorPrueba:
            self.str_Estatus = 'FAILED'
            self.str_Mensaje = errorPrueba

    def test_predict_df_compare(self):

        __str_RutaScripts = os.path.abspath(os.path.curdir)

        __df1 = pd.read_csv(__str_RutaScripts + '/Predict.csv')
        __df2 = pd.read_csv(__str_RutaScripts + '/DatasetModelado.csv')
        __m = __df1.shape[0]==__df2.shape[0]
        __str_note = 'los df no tiene el mismo numero de renglones'

        self.str_NombreMetodo = 'test_predict_df_compare'
        self.dt_HoraEjec = datetime.now()

        try:
            self.assertTrue(__m, note=__str_note)
            self.str_Estatus = 'OK'
        except BaseException as errorPrueba:
            self.str_Estatus = 'FAILED'
            self.str_Mensaje = errorPrueba
