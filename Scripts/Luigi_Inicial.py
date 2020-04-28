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


class Task_05_CrearBD(luigi.Task):

    def run(self):
        # Si puede crear la base bien, generamos el archivo output
        if lt.CrearDB() == 0:
            os.system('echo OK > Task_05_CrearBD')

    def output(self):
        return luigi.LocalTarget('Task_05_CrearBD')


class Task_10_CrearDirectoriosEC2(luigi.Task):

    def requires(self):
        return Task_05_CrearBD()

    def run(self):
        # Si puede crear los directorios bien, generamos el archivo output
        if lt.CrearDirectoriosEC2() == 0:
            os.system('echo OK > Task_10_CrearDirectoriosEC2')

    def output(self):
        return luigi.LocalTarget('Task_10_CrearDirectoriosEC2')


class Task_20_CrearDirectoriosS3(luigi.Task):

    def requires(self):
        return Task_10_CrearDirectoriosEC2()

    def run(self):

        if lt.CrearDirectoriosS3() == 0:
            os.system('echo OK > Task_20_CrearDirectoriosS3')

    def output(self):
        return luigi.LocalTarget('Task_20_CrearDirectoriosS3')


class Task_30_CrearSchemasRDS(luigi.Task):

    def requires(self):
        return Task_20_CrearDirectoriosS3()

    def run(self):
        if lt.CrearSchemasRDS() == 0:
            os.system('echo OK > Task_30_CrearSchemasRDS')

    def output(self):
        return luigi.LocalTarget('Task_30_CrearSchemasRDS')


class Task_40_CrearTablasRDS(luigi.Task):

    def requires(self):
        return Task_30_CrearSchemasRDS()

    def run(self):
        if lt.CrearTablasRDS() == 0:
            os.system('echo OK > Task_40_CrearTablasRDS')

    def output(self):
        return luigi.LocalTarget('Task_40_CrearTablasRDS')


class Task_50_WebScrapingInicial(luigi.Task):

    def requires(self):
        return Task_40_CrearTablasRDS()

    def run(self):
        if lt.WebScrapingInicial() == 0:
            os.system('echo OK > Task_50_WebScrapingInicial')

    def output(self):
        return luigi.LocalTarget('Task_50_WebScrapingInicial')


class Task_60_EnviarMetadataCargaPt1_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return Task_50_WebScrapingInicial()

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

        # os.system('rm Linaje/Ejecuciones/*.csv')
        print('\n---Fin carga de linaje ejecuciones---\n')


class Task_70_EnviarMetadataCargaPt2_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return Task_60_EnviarMetadataCargaPt1_RDS()

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
        # os.system('rm Linaje/Archivos/*.csv')
        print('\n---Fin carga de linaje archivos---\n')


class Task_80_EnviarMetadataCargaPt3_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return Task_70_EnviarMetadataCargaPt2_RDS()

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
        # os.system('rm Linaje/ArchivosDet/*.csv')
        print('\n---Fin carga de linaje archivos_det---\n')


class Task_90_HacerFeatureEngineering(luigi.Task):

    def requires(self):
        return Task_80_EnviarMetadataCargaPt3_RDS()

    def run(self):
        if lt.HacerFeatureEngineering() == 0:
            os.system('echo OK > Task_65_HacerFeatureEngineering')

    def output(self):
        return luigi.LocalTarget('Task_65_HacerFeatureEngineering')


class Task_100_EnviarMetadataFeatureEngineering_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return Task_90_HacerFeatureEngineering()

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
        # os.system('rm Linaje/Transform/*.csv')
        print('\n---Fin carga de linaje transform---\n')


class Task_110_Modelar(luigi.Task):

    def requires(self):
        return Task_100_EnviarMetadataFeatureEngineering_RDS()

    def run(self):
        if lt.Modelar() == 0:
            os.system('echo OK > Task_68_Modelar')

    def output(self):
        return luigi.LocalTarget('Task_68_Modelar')


class Task_120_EnviarMetadataModelado_RDS(luigi.contrib.postgres.CopyToTable):

    def requires(self):
        return Task_110_Modelar()

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
        # os.system('rm Linaje/Transform/*.csv')
        print('\n---Fin carga de linaje modeling---\n')


if __name__ == '__main__':
    luigi.run()
