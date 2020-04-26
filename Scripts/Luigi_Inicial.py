# Librerias de python
import luigi
import os
import time

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


class Task_60_EnviarMetadataLinajeCargaRDS(luigi.Task):

    def requires(self):
        return Task_50_WebScrapingInicial()

    def run(self):
        if lt.EnviarMetadataLinajeCargaRDS() == 0:
            os.system('echo OK > Task_60_EnviarMetadataLinajeCargaRDS')

    def output(self):
        return luigi.LocalTarget('Task_60_EnviarMetadataLinajeCargaRDS')

class Task_65_HacerFeatureEngineering(luigi.Task):

    def requires(self):
        return Task_60_EnviarMetadataLinajeCargaRDS()

    def run(self):
        if lt.HacerFeatureEngineering() == 0:
            os.system('echo OK > Task_65_HacerFeatureEngineering')

    def output(self):
        return luigi.LocalTarget('Task_65_HacerFeatureEngineering')

class Task_67_EnviarMetadataLinajeTransformRDS(luigi.Task):

    def requires(self):
        return Task_65_HacerFeatureEngineering()

    def run(self):
        if lt.EnviarMetadataLinajeTransformRDS() == 0:
            os.system('echo OK > Task_67_EnviarMetadataLinajeTransformRDS')

    def output(self):
        return luigi.LocalTarget('Task_67_EnviarMetadataLinajeTransformRDS')


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


class Tarea_70(luigi.Task):

    def requires(self):
        return Tarea_40()

    def run(self):
        if lt.WebScrapingRecurrente() == 0:
            os.system('echo OK > Tarea_70')

    def output(self):
        return luigi.LocalTarget('Tarea_70')


class Tarea_80(luigi.Task):

    def requires(self):
        return Tarea_70()

    def run(self):
        if lt.EnviarMetadataLinajeRDS() == 0:
            os.system('echo OK > Tarea_80')

    def output(self):
        return luigi.LocalTarget('Tarea_80')


if __name__ == '__main__':
    luigi.run()
