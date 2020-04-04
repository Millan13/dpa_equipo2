# Librerias de python
import luigi
import os
import time

# Librerias de nosotros
from Auxiliar import Auxiliar
import Luigi_Tasks as lt

class Tarea_10(luigi.Task):

    def run(self):
        # Si puede crear los directorios bien, generamos el archivo output
        if lt.CrearDirectoriosEC2() == 0:
            os.system('echo OK > Tarea_10')

    def output(self):
        return luigi.LocalTarget('Tarea_10')

class Tarea_20(luigi.Task):

    def requires(self):
        return Tarea_10()

    def run(self):

        if lt.CrearDirectoriosS3() == 0:
            os.system('echo OK > Tarea_20')

    def output(self):
        return luigi.LocalTarget('Tarea_20')

class Tarea_30(luigi.Task):

    def requires(self):
        return Tarea_20()

    def run(self):
        if lt.CrearSchemasRDS() == 0:
            os.system('echo OK > Tarea_30')

    def output(self):
        return luigi.LocalTarget('Tarea_30')

class Tarea_40(luigi.Task):

    def requires(self):
        return Tarea_30()

    def run(self):
        if lt.CrearTablasLinajeRDS() == 0:
            os.system('echo OK > Tarea_40')

    def output(self):
        return luigi.LocalTarget('Tarea_40')

class Tarea_50(luigi.Task):

    def requires(self):
        return Tarea_40()

    def run(self):
        if lt.WebScrapingInicial() == 0:
            os.system('echo OK > Tarea_50')

    def output(self):
        return luigi.LocalTarget('Tarea_50')

class Tarea_60(luigi.Task):

    def requires(self):
        return Tarea_50()

    def run(self):
        if lt.EnviarMetadataLinajeRDS() == 0:
            os.system('echo OK > Tarea_60')

    def output(self):
        return luigi.LocalTarget('Tarea_60')


if __name__ == '__main__':
    luigi.run()
