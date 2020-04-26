# Librerias de python
import luigi
import os

# Librerias de nosotros
import Luigi_Tasks as lt


class Tarea_70_WebScrapingRecurrente(luigi.Task):

    def requires(self):
        return Task_40_CrearTablasRDS()

    def run(self):
        if lt.WebScrapingRecurrente() == 0:
            os.system('echo OK > Tarea_70_WebScrapingRecurrente')

    def output(self):
        return luigi.LocalTarget('Tarea_70_WebScrapingRecurrente')


class Task_80_EnviarMetadataLinajeCargaRDS(luigi.Task):

    def requires(self):
        return Tarea_70_WebScrapingRecurrente()

    def run(self):
        if lt.EnviarMetadataLinajeCargaRDS() == 0:
            os.system('echo OK > Task_80_EnviarMetadataLinajeCargaRDS')

    def output(self):
        return luigi.LocalTarget('Task_80_EnviarMetadataLinajeCargaRDS')

class Task_85_HacerFeatureEngineering(luigi.Task):

    def requires(self):
        return Task_80_EnviarMetadataLinajeCargaRDS()

    def run(self):
        if lt.HacerFeatureEngineering() == 0:
            os.system('echo OK > Task_85_HacerFeatureEngineering')

    def output(self):
        return luigi.LocalTarget('Task_85_HacerFeatureEngineering')

class Task_87_EnviarMetadataLinajeTransformRDS(luigi.Task):

    def requires(self):
        return Task_65_HacerFeatureEngineering()

    def run(self):
        if lt.EnviarMetadataLinajeTransformRDS() == 0:
            os.system('echo OK > Task_87_EnviarMetadataLinajeTransformRDS')

    def output(self):
        return luigi.LocalTarget('Task_87_EnviarMetadataLinajeTransformRDS')


if __name__ == '__main__':
    luigi.run()
