# Librerias de python
import luigi
import os
import time
import luigi.contrib.postgres
from dynaconf import settings
from pathlib import Path

# Librerias de nosotros
import Luigi_Tasks as lt
from Utileria import Utileria


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


class Task_60_EnviarMetadataLinajeCargaRDS(luigi.contrib.postgres.CopyToTable):

    print('\n---Inicio carga de linaje carga Ejecuciones---\n')
    def requires(self):
        return Task_50_WebScrapingInicial()

    credentials = pd.read_csv("postgres_credentials.csv")
    user = credentials.user[0]
    password = credentials.password[0]
    database = credentials.database[0]
    host = credentials.host[0]

    table = 'linaje.ejecuciones'

    columns = [("id_ejec", "NUMERIC"),\
               ("usuario_ejec", "VARCHAR"),\
               ("instancia_ejec", "VARCHAR"),\
               ("fecha_hora_ejec", "TIMESTAMP"),\
               ("bucket_s3", "VARCHAR"),\
               ("tag_script", "VARCHAR"),\
               ("tipo_ejec", "VARCHAR"),\
               ("url_webscrapping", "VARCHAR"),\
               ("status_ejec", "VARCHAR")]

    def rows(self):
        for data_file in Path('Linaje/Ejecuciones').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file,header= None)
                print(type(reader))
                print(reader)
                for filas in reader.itertuples(index= False):
                    print(filas)
                    yield filas
        os.system('rm Linaje/Ejecuciones/*.csv')
        print('\n---Fin carga de linaje ejecuciones---\n')

        
class Task_61_EnviarMetadataLinajeCargaRDS(luigi.contrib.postgres.CopyToTable):
    print('\n---Inicio carga de linaje carga Archivos---\n')
    def requires(self):
        return Task_60_EnviarMetadataLinajeCargaRDS()

    credentials = pd.read_csv("postgres_credentials.csv")
    user = credentials.user[0]
    password = credentials.password[0]
    database = credentials.database[0]
    host = credentials.host[0]

    columns = [("id_ejec", "NUMERIC"),\
               ("id_archivo", "VARCHAR"),\
               ("num_registros", "VARCHAR"),\
               ("num_columnas", "NUMERIC"),\
               ("tamanio_archivo", "VARCHAR"),\
               ("anio", "VARCHAR"),\
               ("mes", "VARCHAR"),\
               ("ruta_almac_s3", "VARCHAR")]
    table = 'linaje.archivos'
    def rows(self):
        for data_file in Path('Linaje/Archivos').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file,header= None)
                print(type(reader))
                print(reader)
                for filas in reader.itertuples(index= False):
                    print(filas)
                    yield filas
        os.system('rm Linaje/Archivos/*.csv')
        print('\n---Fin carga de linaje archivos---\n')

        
class Task_62_EnviarMetadataLinajeCargaRDS(luigi.contrib.postgres.CopyToTable):
    print('\n---Inicio carga de linaje carga ArchivosDet---\n')
    def requires(self):
        return Task_61_EnviarMetadataLinajeCargaRDS()

    credentials = pd.read_csv("postgres_credentials.csv")
    user = credentials.user[0]
    password = credentials.password[0]
    database = credentials.database[0]
    host = credentials.host[0]

    columns = [("id_archivo", "VARCHAR"),\
               ("nombre_col", "VARCHAR")]
    table = 'linaje.archivos_det'
    def rows(self):
        for data_file in Path('Linaje/ArchivosDet').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file,header= None)
                print(type(reader))
                print(reader)
                for filas in reader.itertuples(index= False):
                    print(filas)
                    yield filas
        os.system('rm Linaje/ArchivosDet/*.csv')
        print('\n---Fin carga de linaje archivos_det---\n')


class Task_65_HacerFeatureEngineering(luigi.Task):

    def requires(self):
        return Task_62_EnviarMetadataLinajeCargaRDS()

    def run(self):
        if lt.HacerFeatureEngineering() == 0:
            os.system('echo OK > Task_65_HacerFeatureEngineering')

    def output(self):
        return luigi.LocalTarget('Task_65_HacerFeatureEngineering')


class Task_67_EnviarMetadataLinajeTransformRDS(luigi.contrib.postgres.CopyToTable):
    print('\n---Inicio carga de linaje transform---\n')
    def requires(self):
        return Task_65_HacerFeatureEngineering()

    credentials = pd.read_csv("postgres_credentials.csv")
    user = credentials.user[0]
    password = credentials.password[0]
    database = credentials.database[0]
    host = credentials.host[0]

    columns = [("id_set_transform", "NUMERIC"),\
               ("num_seq", "NUMERIC"),\
               ("nombre_query","VARCHAR"),\
               ("filas_afectadas","VARCHAR "),\
               ("fecha_hora_ejec","TIMESTAMP"),\
               ("usuario_ejec","VARCHAR"),\
               ("instancia_ejec","VARCHAR")]
    table = 'linaje.transform'
    def rows(self):
        for data_file in Path('Linaje/Transform').glob('*.csv'):
            with open(data_file, 'r') as csv_file:
                reader = pd.read_csv(csv_file,header= None)
                print(type(reader))
                print(reader)
                for filas in reader.itertuples(index= False):
                    print(filas)
                    yield filas
        os.system('rm Linaje/Transform/*.csv')
        print('\n---Fin carga de linaje transform---\n')


class Task_68_Modelar(luigi.Task):

    def requires(self):
        return Task_67_EnviarMetadataLinajeTransformRDS()

    def run(self):
        if lt.Modelar() == 0:
            os.system('echo OK > Task_68_Modelar')

    def output(self):
        return luigi.LocalTarget('Task_68_Modelar')

class Task_69_EnviarMetadataModelingRDS(luigi.Task):

        def requires(self):
            return Task_68_Modelar()

        def run(self):
            objUtileria = Utileria()
            if lt.EnviarMetadataModelingRDS() == 0:
                os.system('echo OK > Task_69_EnviarMetadataModelingRDS')
                objUtileria.DibujarLuigi()
                time.sleep(5)

        def output(self):
            return luigi.LocalTarget('Task_69_EnviarMetadataModelingRDS')


if __name__ == '__main__':
    luigi.run()
