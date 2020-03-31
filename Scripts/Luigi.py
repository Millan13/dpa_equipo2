import luigi
import os
import time

from RitaWebScrap import Auxiliar

class Tarea_05(luigi.Task):
    param = luigi.Parameter(default='5')

    def run(self):
        #os.system('python3 07_create_S3_structure.py')
        os.system('sh 05_create_dirs.sh')
        os.system('echo OK > Tarea_05')

    def output(self):
        str_DirTrabajo=Auxiliar().ObtenerDirectorioTrabajo()
        print('output Tarea5'+str_DirTrabajo)
        return luigi.LocalTarget('Tarea_05')

class Tarea_07(luigi.Task):
    param = luigi.Parameter(default='7')

    def requires(self):
        return Tarea_05(self.param)

    def run(self):
        print('Tarea7 inicio')
        os.system('python3 07_create_S3_structure.py')
        os.system('echo OK > Tarea_07')
        print('Fin Tarea7')

    def output(self):
        str_DirTrabajo=Auxiliar().ObtenerDirectorioTrabajo()
        return luigi.LocalTarget('Tarea_07')

class Tarea_10(luigi.Task):
    param = luigi.Parameter(default='10')

    def requires(self):
        return Tarea_07(self.param)

    def run(self):
        os.system("python3 10_web_Scraping_Rita.py")
        time.sleep(5)
        os.system('echo OK > Tarea_10')

    def output(self):
        str_DirTrabajo=Auxiliar().ObtenerDirectorioTrabajo()
        return luigi.LocalTarget(str_DirTrabajo+'Tarea_10')

if __name__ == '__main__':
    luigi.run()
