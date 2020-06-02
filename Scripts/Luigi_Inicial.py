# Librerias de python
import luigi
import os
import time
import luigi.contrib.postgres
# from dynaconf import settings
from pathlib import Path
import pandas as pd

# Librerias de nosotros
import Luigi_Tasks as lt
import Unit_Tests as ut
from Class_Utileria import Utileria
from Class_Rita import Rita


class T_010_CrearBD(luigi.Task):

    def run(self):
        # Si puede crear la base bien, generamos el archivo output
        if lt.CrearDB() == 0:
            os.system('echo OK > T_010_CrearBD')

    def output(self):
        return luigi.LocalTarget('T_010_CrearBD')


class T_020_CrearDirectoriosEC2(luigi.Task):

    def requires(self):
        return T_010_CrearBD()

    def run(self):
        # Si puede crear los directorios bien, generamos el archivo output
        if lt.CrearDirectoriosEC2() == 0:
            os.system('echo OK > T_020_CrearDirectoriosEC2')

    def output(self):
        return luigi.LocalTarget('T_020_CrearDirectoriosEC2')


class T_030_CrearDirectoriosS3(luigi.Task):

    def requires(self):
        return T_020_CrearDirectoriosEC2()

    def run(self):

        if lt.CrearDirectoriosS3() == 0:
            os.system('echo OK > T_030_CrearDirectoriosS3')

    def output(self):
        return luigi.LocalTarget('T_030_CrearDirectoriosS3')


class T_040_CrearSchemasRDS(luigi.Task):

    def requires(self):
        return T_030_CrearDirectoriosS3()

    def run(self):
        if lt.CrearSchemasRDS() == 0:
            os.system('echo OK > T_040_CrearSchemasRDS')

    def output(self):
        return luigi.LocalTarget('T_040_CrearSchemasRDS')


class T_050_CrearTablasRDS(luigi.Task):

    def requires(self):
        return T_040_CrearSchemasRDS()

    def run(self):
        if lt.CrearTablasRDS() == 0:
            os.system('echo OK > T_050_CrearTablasRDS')

    def output(self):
        return luigi.LocalTarget('T_050_CrearTablasRDS')


class T_060_WebScrapingInicial(luigi.Task):

    def requires(self):
        return T_050_CrearTablasRDS()

    def run(self):
        if lt.WebScrapingInicial() == 0:
            os.system('echo OK > T_060_WebScrapingInicial')

    def output(self):
        return luigi.LocalTarget('T_060_WebScrapingInicial')


class T_070_EnviarMetadataCargaPt1_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return T_060_WebScrapingInicial()

    # Instanciamos la clase Rita
    objRita = Rita()

    # Parámetros de conexión a la RDS
    user, password, database, host = objRita.objUtileria.ObtenerParametrosRDS()

    # Tabla y columnas que se actualizarán
    table = 'linaje.ejecuciones'
    columns = objRita.lst_Ejecuciones

    def rows(self):
        print('\n---Inicio carga de linaje carga Ejecuciones---\n')

        for data_file in Path('Linaje/Ejecuciones').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file, header=None)
                for fila in reader.itertuples(index=False):
                    yield fila

        os.system('rm Linaje/Ejecuciones/*.csv')
        print('\n---Fin carga de linaje ejecuciones---\n')


class T_080_EnviarMetadataCargaPt2_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return T_070_EnviarMetadataCargaPt1_RDS()

    # Instanciamos la clase Rita
    objRita = Rita()

    # Parámetros de conexión a la RDS
    user, password, database, host = objRita.objUtileria.ObtenerParametrosRDS()

    # Tabla y columnas que se actualizarán
    table = 'linaje.archivos'
    columns = objRita.lst_Archivos

    def rows(self):
        print('\n---Inicio carga de linaje carga Archivos---\n')
        for data_file in Path('Linaje/Archivos').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file, header=None)
                for fila in reader.itertuples(index=False):
                    yield fila
        os.system('rm Linaje/Archivos/*.csv')
        print('\n---Fin carga de linaje archivos---\n')


class T_090_EnviarMetadataCargaPt3_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return T_080_EnviarMetadataCargaPt2_RDS()

    # Instanciamos la clase Rita
    objRita = Rita()

    # Parámetros de conexión a la RDS
    user, password, database, host = objRita.objUtileria.ObtenerParametrosRDS()

    # Tabla y columnas que se actualizarán
    table = 'linaje.archivos_det'
    columns = objRita.lst_ArchivosDet

    def rows(self):
        print('\n---Inicio carga de linaje carga ArchivosDet---\n')

        for data_file in Path('Linaje/ArchivosDet').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file, header=None)
                for fila in reader.itertuples(index=False):
                    yield fila
        os.system('rm Linaje/ArchivosDet/*.csv')
        print('\n---Fin carga de linaje archivos_det---\n')


class T_096_UT_Load(luigi.Task):

    def requires(self):
        return T_090_EnviarMetadataCargaPt3_RDS()

    def run(self):
        ut.UT_Load()
        os.system('echo OK > T_096_UT_Load')

    def output(self):
        return luigi.LocalTarget('T_096_UT_Load')


class T_100_HacerFeatureEngineering(luigi.Task):

    def requires(self):
        return T_096_UT_Load()

    def run(self):
        if lt.HacerFeatureEngineering('train') == 0:
            os.system('echo OK > T_100_HacerFeatureEngineering')

    def output(self):
        return luigi.LocalTarget('T_100_HacerFeatureEngineering')


class T_110_EnviarMetadataFeatureEngineering_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return T_100_HacerFeatureEngineering()

    # Instanciamos la clase Rita
    objRita = Rita()

    # Parámetros de conexión a la RDS
    user, password, database, host = objRita.objUtileria.ObtenerParametrosRDS()

    # Tabla y columnas que se actualizarán
    table = 'linaje.transform'
    columns = objRita.lst_Transform

    def rows(self):
        print('\n---Inicio carga de linaje transform---\n')
        for data_file in Path('Linaje/Transform').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file, header=None)
                for fila in reader.itertuples(index=False):
                    yield fila
        os.system('rm Linaje/Transform/*.csv')
        print('\n---Fin carga de linaje transform---\n')


class T_115_UT_Transform(luigi.Task):

    def requires(self):
        return T_110_EnviarMetadataFeatureEngineering_RDS()

    def run(self):
        ut.UT_Transform()
        os.system('echo OK > T_115_UT_Transform')

    def output(self):
        return luigi.LocalTarget('T_115_UT_Transform')

class T_200_UT_Predict(luigi.Task):

    def requires(self):
        return T_115_UT_Transform()

    def run(self):
        ut.UT_Predict()
        os.system('echo OK > T_200_UT_Predict')

    def output(self):
        return luigi.LocalTarget('T_200_UT_Predict')



# on_succes permite hacer override
class T_120_Modelar(luigi.Task):

    def requires(self):
        return T_115_UT_Transform()

    def run(self):
        if lt.Modelar() == 0:
            os.system('echo OK > T_120_Modelar')

    def output(self):
        return luigi.LocalTarget('T_120_Modelar')


class T_130_EnviarMetadataModelado_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return T_120_Modelar()

    # Instanciamos la clase Rita
    objRita = Rita()

    # Parámetros de conexión a la RDS
    user, password, database, host = objRita.objUtileria.ObtenerParametrosRDS()

    # Tabla y columnas que se actualizarán
    table = 'linaje.modeling'
    columns = objRita.lst_Modeling

    def rows(self):
        print('\n---Inicio carga de linaje modeling---\n')
        for data_file in Path('Linaje/Modeling').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file, header=None)
                for fila in reader.itertuples(index=False):
                    yield fila
        os.system('rm Linaje/Modeling/*.csv')
        print('\n---Fin carga de linaje modeling---\n')
        # self.objRita.objUtileria.DibujarLuigi()
        # time.sleep(4)

class T_180_Predict(luigi.Task):

    def requires(self):
        return T_130_EnviarMetadataModelado_RDS()

    def run(self):
        if lt.Predict() == 0:
            os.system('echo OK > T_180_Predict')

    def output(self):
        return luigi.LocalTarget('T_180_Predict')


class T_190_Enviar_Predict_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return T_180_Predict()

    # Instanciamos la clase Rita
    objRita = Rita()

    # Parámetros de conexión a la RDS
    user, password, database, host = objRita.objUtileria.ObtenerParametrosRDS()

    # Tabla y columnas que se actualizarán
    table = 'trabajo.predicciones'
    columns = objRita.lst_Predicciones

    def rows(self):
        print('\n---Inicio carga de Predicciones---\n')

        for data_file in Path('.').glob('Predicciones.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file, header=None)
                for fila in reader.itertuples(index=False):
                    yield fila
        # os.system('rm Predicciones.csv')
        print('\n---Fin carga de Predicciones---\n')


class T_140_PrepararScheduleVuelos(luigi.Task):

    def requires(self):
        return T_130_EnviarMetadataModelado_RDS()

    def run(self):
        if lt.PrepararScheduleVuelos() == 0:
            os.system('echo OK > T_140_PrepararScheduleVuelos')

    def output(self):
        return luigi.LocalTarget('T_140_PrepararScheduleVuelos')


class T_150_FeatureEngineering_Predict(luigi.Task):
    def requires(self):
        return T_140_PrepararScheduleVuelos()

    def run(self):
        if lt.HacerFeatureEngineering('test') == 0:
            os.system('echo OK > T_150_FeatureEngineering_Predict')

    def output(self):
        return luigi.LocalTarget('T_150_FeatureEngineering_Predict')


class T_160_MetadataFeatureEngineering_Predict(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return T_150_FeatureEngineering_Predict()

    # Instanciamos la clase Rita
    objRita = Rita()

    # Parámetros de conexión a la RDS
    user, password, database, host = objRita.objUtileria.ObtenerParametrosRDS()

    # Tabla y columnas que se actualizarán
    table = 'linaje.transform'
    columns = objRita.lst_Transform

    def rows(self):
        print('\n---Inicio carga de linaje transform---\n')
        for data_file in Path('Linaje/Transform').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file, header=None)
                for fila in reader.itertuples(index=False):
                    yield fila
        os.system('rm Linaje/Transform/*.csv')
        print('\n---Fin carga de linaje transform---\n')


class T_170_UT_TransformPredict(luigi.Task):

    def requires(self):
        return T_160_MetadataFeatureEngineering_Predict()

    def run(self):
        ut.UT_Transform_Predict()
        os.system('echo OK > T_170_UT_TransformPredict')

    def output(self):
        return luigi.LocalTarget('T_170_UT_TransformPredict')



class Tarea_10_WebScrapingScheduleVuelos(luigi.Task):

    def run(self):
        if lt.WebScrapingScheduleVuelos() == 0:
            os.system('echo OK > Tarea_10_WebScrapingScheduleVuelos')

    def output(self):
        return luigi.LocalTarget('Tarea_10_WebScrapingScheduleVuelos')

if __name__ == '__main__':
    luigi.run()


class Tarea_20_EnviarMetadataLinajeScheduleCargaRDS(luigi.Task):

    def requires(self):
        return Tarea_10_WebScrapingScheduleVuelos()

    def run(self):
        if lt.EnviarMetadataLinajeCargaRDS() == 0:
            os.system('echo OK > Tarea_20_EnviarMetadataLinajeScheduleCargaRDS')

    def output(self):
        return luigi.LocalTarget('Tarea_20_EnviarMetadataLinajeScheduleCargaRDS')

# ##################### Task principal de todo el flujo #####################
class T_Manejador(luigi.Task):

    str_Tarea = luigi.Parameter()

    def requires(self):

        # Diccionarios que tienen todas las clases que pueden ser llamadas
        # dependiendo de los parámetros recibidos
        dict_LT = {'010': {'Clase': T_010_CrearBD()},
                   '020': {'Clase': T_020_CrearDirectoriosEC2()},
                   '030': {'Clase': T_030_CrearDirectoriosS3()},
                   '040': {'Clase': T_040_CrearSchemasRDS()},
                   '050': {'Clase': T_050_CrearTablasRDS()},
                   '060': {'Clase': T_060_WebScrapingInicial()},
                   '070': {'Clase': T_070_EnviarMetadataCargaPt1_RDS()},
                   '080': {'Clase': T_080_EnviarMetadataCargaPt2_RDS()},
                   '090': {'Clase': T_090_EnviarMetadataCargaPt3_RDS()},
                   '096': {'Clase': T_096_UT_Load()},  # Unit Test
                   '100': {'Clase': T_100_HacerFeatureEngineering()},
                   '110': {'Clase': T_110_EnviarMetadataFeatureEngineering_RDS()},
                   '115': {'Clase': T_115_UT_Transform()},  # Unit Test
                   '120': {'Clase': T_120_Modelar()},
                   '130': {'Clase': T_130_EnviarMetadataModelado_RDS()},
                   '140': {'Clase': T_140_PrepararScheduleVuelos()},
                   '150': {'Clase': T_150_FeatureEngineering_Predict()},
                   '160': {'Clase': T_160_MetadataFeatureEngineering_Predict()},
                   '170': {'Clase': T_170_UT_TransformPredict()},
                   '180': {'Clase': T_180_Predict()},
                   '190': {'Clase': T_190_Enviar_Predict_RDS()}
                   }

        # Ejemplo: return dict_LT.get('010').get('Clase')
        return dict_LT.get(self.str_Tarea).get('Clase')

    def run(self):
        Utileria().DibujarLuigi()
        time.sleep(4)


if __name__ == '__main__':
    luigi.run()
