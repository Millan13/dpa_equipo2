import luigi
import os
import time

def obtenerDirectorioTrabajo():

    str_Ambiente='Local'
    #str_Ambiente='EC2'

    str_DirTrabajo=''
    if str_Ambiente=='Local':
        str_DirTrabajo='/Users/Marco/Ciencia_de_Datos/Maestria/2do_Semestre/Liliana/Pruebas/'
    elif str_Ambiente=='EC2':
        str_DirTrabajo='/home/ec2-user/'

    return str_DirTrabajo

class Tarea_05(luigi.Task):
    param = luigi.Parameter(default=40)

    def run(self):
        os.system('sh 05_create_dirs.sh')
        os.system('echo OK > Tarea_05')

    def output(self):
        str_DirTrabajo=obtenerDirectorioTrabajo()
        return luigi.LocalTarget(str_DirTrabajo+'Tarea_05')

class Tarea_10(luigi.Task):
    param = luigi.Parameter(default=41)

    def requires(self):
        return Tarea_05(self.param)

    def run(self):
        os.system("python3 10_web_Scraping_Rita.py")
        time.sleep(5)
        os.system('echo OK > Tarea_10')

    def output(self):
        str_DirTrabajo=obtenerDirectorioTrabajo()
        return luigi.LocalTarget(str_DirTrabajo+'Tarea_10')

class Tarea_20(luigi.Task):
    param = luigi.Parameter(default=42)

    def requires(self):
        return Tarea_10(self.param)

    def run(self):
        os.system('sh 20_unzip_files.sh')
        os.system('echo OK > Tarea_20')

    def output(self):
        str_DirTrabajo=obtenerDirectorioTrabajo()
        return luigi.LocalTarget(str_DirTrabajo+'Tarea_20')

class Tarea_30(luigi.Task):
    param = luigi.Parameter(default=43)

    def requires(self):
        return Tarea_20(self.param)

    def run(self):
        os.system('sh 30_rm_zipfiles.sh')
        os.system('echo OK > Tarea_30')

    def output(self):
        str_DirTrabajo=obtenerDirectorioTrabajo()
        return luigi.LocalTarget(str_DirTrabajo+'Tarea_30')

class Tarea_40(luigi.Task):
    param = luigi.Parameter(default=44)

    def requires(self):
        return Tarea_30(self.param)

    def run(self):
        os.system('sh 40_move_files.sh')
        os.system('echo OK > Tarea_40')

    def output(self):
        str_DirTrabajo=obtenerDirectorioTrabajo()
        return luigi.LocalTarget(str_DirTrabajo+'Tarea_40')

class Tarea_50(luigi.Task):

    param = luigi.Parameter(default=45)

    def requires(self):
        return Tarea_40(self.param)

    def run(self):
        os.system('sh 50_gather_files.sh')
        os.system('echo OK > Tarea_50')

    def output(self):
        str_DirTrabajo=obtenerDirectorioTrabajo()
        return luigi.LocalTarget(str_DirTrabajo+'Tarea_50')

class Tarea_60(luigi.Task):

    param = luigi.Parameter(default=46)

    def requires(self):
        return Tarea_50(self.param)

    def run(self):
        os.system("python3 60_send_to_s3.py")
        os.system('echo OK > Tarea_60')

    def output(self):
        str_DirTrabajo=obtenerDirectorioTrabajo()
        return luigi.LocalTarget(str_DirTrabajo+'Tarea_60')

#class TareaEjemplo(luigi.Task):
#    param = luigi.Parameter(default=42)
#
#    def requires(self):
#        return SomeOtherTask(self.param)
#
#    def run(self):
#        r = self.output().open('w')
#        print >>f, 'HOla mundo'
#        f.close()
#
#    def output(self):
#        return luigi.LocalTarget('/Users/Marco/Ciencia_de_Datos/Maestria/2do_Semestre/Liliana/Pruebas' %  self.parameter)

if __name__ == '__main__':
    luigi.run()
