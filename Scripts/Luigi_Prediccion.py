# Librerias de python
import luigi
import os

# Librerias de nosotros
import Luigi_Tasks as lt


class Tarea_10_WebScrapingRecurrente(luigi.Task):

    def run(self):
        if lt.WebScrapingScheduleVuelos() == 0:
            os.system('echo OK > Tarea_10_WebScrapingScheduleVuelos')

    def output(self):
        return luigi.LocalTarget('Tarea_10_WebScrapingScheduleVuelos')

if __name__ == '__main__':
    luigi.run()
