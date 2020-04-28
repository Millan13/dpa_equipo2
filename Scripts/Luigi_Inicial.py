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
    user = objRita.objUtileria.str_UsuarioDB
    password = objRita.objUtileria.str_PassDB
    database = objRita.objUtileria.str_NombreDB
    host = objRita.objUtileria.str_EndPointDB

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
    user = objRita.objUtileria.str_UsuarioDB
    password = objRita.objUtileria.str_PassDB
    database = objRita.objUtileria.str_NombreDB
    host = objRita.objUtileria.str_EndPointDB

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
    user = objRita.objUtileria.str_UsuarioDB
    password = objRita.objUtileria.str_PassDB
    database = objRita.objUtileria.str_NombreDB
    host = objRita.objUtileria.str_EndPointDB

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
    user = objRita.objUtileria.str_UsuarioDB
    password = objRita.objUtileria.str_PassDB
    database = objRita.objUtileria.str_NombreDB
    host = objRita.objUtileria.str_EndPointDB

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
    user = objRita.objUtileria.str_UsuarioDB
    password = objRita.objUtileria.str_PassDB
    database = objRita.objUtileria.str_NombreDB
    host = objRita.objUtileria.str_EndPointDB

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


if __name__ == '__main__':
    luigi.run()
