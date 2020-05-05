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
# from Class_Utileria import Utileria
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


class T_100_HacerFeatureEngineering(luigi.Task):

    def requires(self):
        return T_090_EnviarMetadataCargaPt3_RDS()

    def run(self):
        if lt.HacerFeatureEngineering() == 0:
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


class T_120_Modelar(luigi.Task):

    def requires(self):
        return T_110_EnviarMetadataFeatureEngineering_RDS()

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
        self.objRita.objUtileria.DibujarLuigi()
        time.sleep(4)


# ##################### Task principal de todo el flujo #####################
class T_Manejador(luigi.Task):

    str_Tipo = luigi.Parameter()
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
                   '100': {'Clase': T_100_HacerFeatureEngineering()},
                   '110': {'Clase': T_110_EnviarMetadataFeatureEngineering_RDS()},
                   '120': {'Clase': T_120_Modelar()},
                   '130': {'Clase': T_130_EnviarMetadataModelado_RDS()},
                   }

        dict_UT = {'010': {'Clase': UT_010_Extract()},
                   '020': {'Clase': UT_020_Metadata_Extract()},
                   '030': {'Clase': UT_030_Load()},
                   '040': {'Clase': UT_040_Metadata_Load()},
                   '050': {'Clase': UT_050_Transform()},
                   '060': {'Clase': UT_060_Metadata_Transform()},
                   '070': {'Clase': UT_070_Modeling()},
                   '080': {'Clase': UT_080_Metadata_Modeling()},
                   }

        if self.str_Tipo == 'LT':

            # Ejemplo: return dict_LT.get('010').get('Clase')
            return dict_LT.get(self.str_Tarea).get('Clase')

        elif self.str_Tipo == 'UT':

            # Ejemplo: return dict_UT.get('010').get('Clase')
            return dict_UT.get(self.str_Tarea).get('Clase')


# ##################### Unit Tests #####################
class UT_010_Extract(luigi.Task):

    def run(self):
        ut.Extract()
        os.system('echo OK > UT_010_Extract')

    def output(self):
        return luigi.LocalTarget('UT_010_Extract')


class UT_020_Metadata_Extract(luigi.Task):

    def requires(self):
        return UT_010_Extract()

    def run(self):
        ut.Metadata_Extract()
        os.system('echo OK > UT_020_Metadata_Extract')

    def output(self):
        return luigi.LocalTarget('UT_020_Metadata_Extract')


class UT_030_Load(luigi.Task):

    def requires(self):
        return UT_020_Metadata_Extract()

    def run(self):
        ut.Load()
        os.system('echo OK > UT_030_Load')

    def output(self):
        return luigi.LocalTarget('UT_030_Load')


class UT_040_Metadata_Load(luigi.Task):

    def requires(self):
        return UT_030_Load()

    def run(self):
        ut.Metadata_Load()
        os.system('echo OK > UT_040_Metadata_Load')

    def output(self):
        return luigi.LocalTarget('UT_040_Metadata_Load')


class UT_050_Transform(luigi.Task):

    def requires(self):
        return UT_040_Metadata_Load()

    def run(self):
        ut.Transform()
        os.system('echo OK > UT_050_Transform')

    def output(self):
        return luigi.LocalTarget('UT_050_Transform')


class UT_060_Metadata_Transform(luigi.Task):

    def requires(self):
        return UT_050_Transform()

    def run(self):
        ut.Metadata_Transform()
        os.system('echo OK > UT_060_Metadata_Transform')

    def output(self):
        return luigi.LocalTarget('UT_060_Metadata_Transform')


class UT_070_Modeling(luigi.Task):

    def requires(self):
        return UT_060_Metadata_Transform()

    def run(self):
        ut.Modeling()
        os.system('echo OK > UT_070_Modeling')

    def output(self):
        return luigi.LocalTarget('UT_070_Modeling')


class UT_080_Metadata_Modeling(luigi.Task):

    def requires(self):
        return UT_070_Modeling()

    def run(self):
        ut.Metadata_Modeling()
        os.system('echo OK > UT_080_Metadata_Modeling')

    def output(self):
        return luigi.LocalTarget('UT_080_Metadata_Modeling')


if __name__ == '__main__':
    luigi.run()
