import luigi
import os
import time

from RitaWebScrap import Auxiliar

class Tarea_05(luigi.Task):
    param = luigi.Parameter(default='5')

    def run(self):
        os.system('sh 05_create_dirs.sh')
        os.system('echo OK > Tarea_05')

    def output(self):
        str_DirTrabajo=Auxiliar().ObtenerDirectorioTrabajo()
        return luigi.LocalTarget('Tarea_05')

class Tarea_07(luigi.Task):
    param = luigi.Parameter(default='7')

    def requires(self):
        return Tarea_05(self.param)

    def run(self):
        os.system('python3 07_create_S3_structure.py')
        os.system('echo OK > Tarea_07')

    def output(self):
        str_DirTrabajo=Auxiliar().ObtenerDirectorioTrabajo()
        return luigi.LocalTarget('Tarea_07')

class Tarea_08(luigi.Task):
    param = luigi.Parameter(default='8')

    def requires(self):
        return Tarea_07(self.param)

    def run(self):
        os.system('export LC_ALL=en_US.UTF-8')
        os.system('python3 rita.py create-schemas')
        os.system('echo OK > Tarea_08')

    def output(self):
        str_DirTrabajo=Auxiliar().ObtenerDirectorioTrabajo()
        return luigi.LocalTarget('Tarea_08')

class Tarea_09(luigi.Task):
    param = luigi.Parameter(default='9')

    def requires(self):
        return Tarea_08(self.param)

    def run(self):
        os.system('export LC_ALL=en_US.UTF-8')
        os.system('python3 rita.py create-linaje-tables')
        os.system('echo OK > Tarea_09')

    def output(self):
        str_DirTrabajo=Auxiliar().ObtenerDirectorioTrabajo()
        return luigi.LocalTarget('Tarea_09')

class Tarea_10(luigi.Task):
    param = luigi.Parameter(default='10')

    def requires(self):
        return Tarea_09(self.param)

    def run(self):
        os.system("python3 10_web_Scraping_Rita.py")
        time.sleep(5)
        os.system('echo OK > Tarea_10')

    def output(self):
        str_DirTrabajo=Auxiliar().ObtenerDirectorioTrabajo()
        return luigi.LocalTarget(str_DirTrabajo+'Tarea_10')

if __name__ == '__main__':
    luigi.run()
