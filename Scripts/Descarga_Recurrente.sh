#!/bin/sh
#script que realiza la descarga recurrente de nueva información
set -x
cd /home/ec2-user/dpa_equipo2/Scripts/
python3 -m luigi --module Luigi_Recurrente Tarea_10_WebScrapingRecurrente --local-scheduler

