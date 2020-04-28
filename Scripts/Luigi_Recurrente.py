# Librerias de python
import luigi
import os

# Librerias de nosotros
import Luigi_Tasks as lt


class Tarea_10_WebScrapingRecurrente(luigi.Task):

    def run(self):
        if lt.WebScrapingRecurrente() == 0:
            os.system('echo OK > Tarea_10_WebScrapingRecurrente')

    def output(self):
        return luigi.LocalTarget('Tarea_10_WebScrapingRecurrente')


class Tarea_20_EnviarMetadataLinajeRecurrenteCargaRDS(luigi.Task):

    def requires(self):
        return Tarea_10_WebScrapingRecurrente()

    def run(self):
        if lt.EnviarMetadataLinajeCargaRDS() == 0:
            os.system('echo OK > Tarea_20_EnviarMetadataLinajeRecurrenteCargaRDS')

    def output(self):
        return luigi.LocalTarget('Tarea_20_EnviarMetadataLinajeRecurrenteCargaRDS')

if __name__ == '__main__':
    luigi.run()
