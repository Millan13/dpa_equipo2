import os
import boto3
from pathlib import Path
from dynaconf import settings
import pandas


class Utileria:

    # Atributos S3
    str_NombreBucket = ''

    # Atributos RDS
    str_NombreDB = ''
    str_UsuarioDB = ''
    str_PassDB = ''
    str_EndPointDb = ''

    def __init__(self):

        # RDS
        self.str_NombreDB = settings.get('dbname')
        self.str_UsuarioDB = settings.get('user')
        self.str_PassDB = settings.get('password')
        self.str_EndPointDB = settings.get('host')

        # S3
        self.str_NombreBucket = settings.get('bucket_name')

    def ObtenerTamanioArchivo(self, str_NombreArchivo):
        import os

        nbr_file_size = os.stat(str_NombreArchivo).st_size
        return nbr_file_size

    def CrearConexionRDS(self):

        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(database=self.str_NombreDB,
                                user=self.str_UsuarioDB,
                                password=self.str_PassDB,
                                host=self.str_EndPointDB,
                                port='5432'
                                )
        return conn

    def ObtenerParametrosRDS(self):

        return (self.str_UsuarioDB,
                self.str_PassDB,
                self.str_NombreDB,
                self.str_EndPointDB)


    def ExisteBaseCreada(self):

        import psycopg2

        bool_YaExiste = False
        str_Query = "SELECT datname FROM pg_database WHERE datname = '" + self.str_NombreDB + "' ;"

        conn = psycopg2.connect(user=self.str_UsuarioDB,
                                host=self.str_EndPointDB,
                                password=self.str_PassDB)

        cur = conn.cursor()
        cur.execute(str_Query)

        if cur.rowcount == 0:
            bool_YaExiste = False
        else:
            bool_YaExiste = True

        return bool_YaExiste

    def ObtenerMaxId(self, conn, str_tabla, str_campo):

        nbr_MaxId = 0
        str_Query = 'select max('+str_campo+') from '+str_tabla+';'

        cur = conn.cursor()
        cur.execute(str_Query)

        if cur.rowcount == 0:
            nbr_MaxId = 0
        else:
            row = cur.fetchone()
            if row[0] is None:
                nbr_MaxId = 0
            else:
                nbr_MaxId = row[0]

        return nbr_MaxId

    def ObtenerUsuario(self):
        import getpass

        return getpass.getuser()

    def ObtenerIp(self):
        import socket

        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        return ip_address

    def CrearConexionS3(self):

        from boto3 import Session
        session = Session()

        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()

        s3 = boto3.client(
                's3',
                aws_access_key_id=current_credentials.access_key,
                aws_secret_access_key=current_credentials.secret_key,
                aws_session_token=current_credentials.token,
                region_name='us-west-2',  # Oregon
                use_ssl=False
            )

        return s3

    def MandarArchivoS3(self, cnx_S3, bucket_name, str_RutaS3, str_Archivo):

        str_ArchivoEnvio = str_Archivo
        str_NombreArchivoEnS3 = str_RutaS3+os.path.basename(str_Archivo)

        # Mandamos el archivo a S3
        print('str_ArchivoEnvio: ', str_ArchivoEnvio)
        print('str_NombreArchivoEnS3: ', str_NombreArchivoEnS3)

        print('Enviando el archivo a S3...')
        cnx_S3.upload_file(str_ArchivoEnvio, bucket_name, str_NombreArchivoEnS3)

        return

    def ObtenerArchivoS3(self, cnx_S3, bucket_name, str_RutaS3):

        return

    def ObtenerQueries(self, str_Path):

        queries = {}
        for sql_file in Path(str_Path).glob('*.sql'):
            with open(sql_file, 'r') as sql:
                sql_key = sql_file.stem
                query = str(sql.read())
                queries[sql_key] = query

        return queries

    def InsertarEnRDSDesdeArchivo(self, conn, data_file, nombre_tabla):
        import io

        with conn.cursor() as cursor:
            print('nombre_tabla: ' + nombre_tabla)

            # Armamos la cadena sql concatenando el nombre de la tabla recibido como parámetro
            sql_statement = f"copy " + nombre_tabla + " from stdin with csv delimiter as ','"
            print(sql_statement)
            buffer = io.StringIO()

            with open(data_file, 'r') as data:
                buffer.write(data.read())
            buffer.seek(0)
            cursor.copy_expert(sql_statement, file=buffer)

    def InsertarEnRDSDesdeArchivo2(self, conn, data_file, nombre_tabla):

        cur = conn.cursor()

        # Load table from the file with header
        print("copy {} from STDIN CSV HEADER QUOTE '\"'".format(nombre_tabla))
        cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(nombre_tabla), data_file)
        cur.execute("commit;")

        print("Loaded data into {}".format(nombre_tabla))
        cur.close()


    def EjecutarQuery(self, conn, query):
        try:
            with conn.cursor() as (cur):
                cur.execute(query)
                rowcount = cur.rowcount
            return rowcount
        except Exception:
            print('Excepcion en EjecutarQuery-cur.execute: ', query)
            raise

    def DibujarLuigi(self):

        strLuigi = ''
        strLuigi = strLuigi + "                                           \n"
        strLuigi = strLuigi + "                                           \n"
        strLuigi = strLuigi + "              /(         )\                \n"
        strLuigi = strLuigi + "              \ \       / /                \n"
        strLuigi = strLuigi + "               \ _'''''_ /                 \n"
        strLuigi = strLuigi + "                .   L   .                  \n"
        strLuigi = strLuigi + "               /  .===.  \                 \n"
        strLuigi = strLuigi + "               \/ 6   6 \/                 \n"
        strLuigi = strLuigi + "               (  \___/  )                 \n"
        strLuigi = strLuigi + "   ________OOO__\_______/_____________     \n"
        strLuigi = strLuigi + "  /                                    \   \n"
        strLuigi = strLuigi + " |                                      |  \n"
        strLuigi = strLuigi + " |          Pipeline correcto           |  \n"
        strLuigi = strLuigi + " |                                      |  \n"
        strLuigi = strLuigi + " |                                      |  \n"
        strLuigi = strLuigi + "  \_______________________OOO__________/   \n"
        strLuigi = strLuigi + "                 |  |  |                   \n"
        strLuigi = strLuigi + "                 |_ | _|                   \n"
        strLuigi = strLuigi + "                 |  |  |                   \n"
        strLuigi = strLuigi + "                 |__|__|                   \n"
        strLuigi = strLuigi + "                 /-'Y'-\                   \n"
        strLuigi = strLuigi + "                (__/ \__)                  \n"
        strLuigi = strLuigi + "                                           \n"
        strLuigi = strLuigi + "              -Luigi Deamon-               \n"
        print(strLuigi)

        return
