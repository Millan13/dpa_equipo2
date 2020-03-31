#! /usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras

import sys
from datetime import timedelta

import click

import io

from dynaconf import settings

from pathlib import Path

@click.group()
@click.pass_context
def rita(ctx):
    ctx.ensure_object(dict)
    #conn = psycopg2.connect(settings.get('PGCONNSTRING'))
    conn = psycopg2.connect(database="bd_rita"
                            user="postgres",
                            password="12345678",
                            host="database-2.cnevbxmlm0lp.us-west-2.rds.amazonaws.com",
                            port='5432'
    )

    conn.autocommit = True
    ctx.obj['conn'] = conn

    queries = {}
    for sql_file in Path('sql').glob('*.sql'):
        with open(sql_file,'r') as sql:
            sql_key = sql_file.stem
            query = str(sql.read())
            queries[sql_key] = query
    ctx.obj['queries'] = queries

@rita.command()
@click.pass_context
def create_schemas(ctx):
    query = ctx.obj['queries'].get('create_schemas')
    conn = ctx.obj['conn']
    with conn.cursor() as cur:
        cur.execute(query)


@rita.command()
@click.pass_context
def create_linaje_tablas(ctx):
    query = ctx.obj['queries'].get('create_linaje_tablas')
    conn = ctx.obj['conn']
    with conn.cursor() as cur:
        cur.execute(query)


@rita.command()
@click.pass_context
def load_rita(ctx):
    for data_file in Path('data_rita').glob('*.tsv'):
        print(data_file)
        table = data_file.stem

        #Puesto que existen archivos que por su tamaño fueron separados, ejecutaremos un split para identificar si estamos
        #leyendo un archivo segmentado o no
        x = table.split('-')

        #Si sólo hay un elemento, significa que se trata de un archivo no segmentado
        if len(x) == 1:

            #Mandamos a llamar a la función genérica que carga los datos en la tabla deseada
            cargar_tabla(ctx, data_file, table)

        #Si hay más de un elemento, significa que se trata de un archivo resultado de una segmentación.
        else:

            #En este caso, el nombre de la tabla se encuentra en el índice 1
            table = x[1]

            #Mandamos a llamar a la función genérica que carga los datos en la tabla deseada
            cargar_tabla(ctx, data_file, table)


############# Cargar Tabla #############
#
#   Se crea esta función auxiliar que recibirá archivos como fuentes de datos, así como el nombre de la tabla donde deberán ser depositados
#
#   Parámetros
#       ctx:            contexto
#       data_file:      archivo que contiene los datos de una tabla (origen de datos)
#       nombre_tabal:   nombre de la tabla donde se dopositarán los datos (destino de los datos)
#   Regresa
#       nada
#
#######################################
def cargar_tabla(ctx, data_file, nombre_tabla):
    conn = ctx.obj['conn']
    with conn.cursor() as cursor:
        print(nombre_tabla)

        #Armamos la cadena sql concatenando el nombre de la tabla recibido como parámetro
        sql_statement = f"copy linaje." + nombre_tabla + " from stdin with csv delimiter as '\t'"
        print(sql_statement)
        buffer = io.StringIO()
        with open(data_file,'r') as data:
            buffer.write(data.read())
        buffer.seek(0)
        cursor.copy_expert(sql_statement, file=buffer)

if __name__ == '__main__':
    rita()
