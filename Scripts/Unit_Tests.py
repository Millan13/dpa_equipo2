import os

# Librerias de nosotros
from Class_Utileria import Utileria
# from testing import TestPandas
# ###############################################################
# ################### Funciones principales #####################
# ###############################################################


def UT_Load():

    print('\n---Inicio UT_Load ---\n')
    from testing import test_load as tst

    obj_UT = tst.TestLoad()

    # PRUEBA 1
    # Se realiza la prueba unitaria y se procesa la metadata
    obj_UT.test_load_count_columns()
    procesar_metadata_unit_test(obj_UT, 'testing/Load/')
    print('\n---Fin UT_Load ---\n')


def UT_Transform():

    print('\n---Inicio UT_Transform ---\n')
    from testing import test_transform as tst

    obj_UT = tst.TestTransform()

    obj_UT.test_transform_delay_positive()
    procesar_metadata_unit_test(obj_UT, 'testing/Transform/')

    print('\n---Fin UT_Transform ---\n')

def UT_Predict():

    print('\n---Inicio UT_Predict ---\n')
    from testing import test_predict as tst

    obj_UT = tst.TestPredict()

    obj_UT.test_predict_delay_binary()
    procesar_metadata_unit_test(obj_UT, 'testing/Predict/')

    obj_UT.test_predict_df_compare()
    procesar_metadata_unit_test(obj_UT, 'testing/Predict/')

    print('\n---Fin UT_Predict ---\n')


def procesar_metadata_unit_test(par_UI, par_Ruta):

    # Se genera la metadata
    generar_archivo_metadata_ut(par_UI, par_Ruta)

    # Se envía a RDS
    enviar_metadata_ut(par_Ruta)

    # Se elimina el archivo con la metadata
    os.system('rm ' + par_Ruta + '*.csv')

    # Se evalúa si hay que detener la ejecución
    validar_continuacion(par_UI)


def generar_archivo_metadata_ut(par_UnitTest,
                                par_Ruta):

    from Class_ValueObjects import voUnitTest

    obj_Utileria = Utileria()
    conn = obj_Utileria.CrearConexionRDS()
    nbr_Id_UnitTest = obj_Utileria.ObtenerMaxId(conn,
                                                'linaje.unit_tests',
                                                'id_unit_test') + 1

    voEjecucion = voUnitTest()
    voEjecucion.nbr_id_unittest = nbr_Id_UnitTest
    voEjecucion.str_nombre_clase = par_UnitTest.__name__
    voEjecucion.str_nombre_metodo = par_UnitTest.str_NombreMetodo
    voEjecucion.str_estatus = par_UnitTest.str_Estatus
    voEjecucion.str_mensaje = par_UnitTest.str_Mensaje
    voEjecucion.dt_hora_ejec = par_UnitTest.dt_HoraEjec
    voEjecucion.str_NombreDataFrame = par_Ruta + par_UnitTest.__name__ \
                                    + '_' + par_UnitTest.str_NombreMetodo \
                                    + '.csv'
    voEjecucion.crearCSV()


def enviar_metadata_ut(par_Ruta_Arch):

    from pathlib import Path
    objUtileria = Utileria()
    cnn = objUtileria.CrearConexionRDS()
    cnn.autocommit = True

    # Barremos los csv de Ejecuciones
    for data_file in Path(par_Ruta_Arch).glob('*.csv'):
        try:
            objUtileria.InsertarEnRDSDesdeArchivo(cnn, data_file, 'linaje.unit_tests')
        except Exception:
            print('Excepcion en Metadata_Extract')
            raise ('Excepción de prueba')


def validar_continuacion(par_UT):

    if par_UT.str_Estatus == 'FAILED':
        raise BaseException(par_UT.str_Mensaje)
