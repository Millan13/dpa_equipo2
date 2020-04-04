class Auxiliar:

    def ObtenerTamanioArchivo(self, str_NombreArchivo):
        import os

        nbr_file_size = os.stat(str_NombreArchivo).st_size
        return nbr_file_size


    def CrearConexionRDS(self):

        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(database="nombre_base"
                               ,user="nombre_usuario"
                               ,password="password"
                               ,host="end_point"
                               ,port='puerto'
                               )
        return conn

    def ObtenerMaxId(self):
        import psycopg2

        nbr_MaxId=0
        str_Query='select max(id_ejec) from linaje.ejecuciones;'

        conn = self.CrearConexionRDS()

        cur = conn.cursor()
        cur.execute(str_Query)

        if  cur.rowcount==0:
            nbr_MaxId=0
        else:
            row=cur.fetchone()
            if row[0] is None:
                nbr_MaxId=0
            else:
                nbr_MaxId=row[0]

        print(nbr_MaxId)
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

        s3=boto3.client(
                's3',
                aws_access_key_id=current_credentials.access_key[1:-1],
                aws_secret_access_key=current_credentials.secret_key[1:-1],
                region_name='us-west-2', #Oregon
                use_ssl=False
            )

        return s3

    def MandarArchivoS3(self, cnx_S3, bucket_name, str_RutaS3, str_Archivo):

        str_ArchivoEnvio=str_Archivo
        str_NombreArchivoEnS3 = str_RutaS3+os.path.basename(str_Archivo)

        # Mandamos el archivo a S3
        print('str_ArchivoEnvio: ', str_ArchivoEnvio)
        print('str_NombreArchivoEnS3: ', str_NombreArchivoEnS3)

        print('Enviando el archivo a S3')
        cnx_S3.upload_file(str_ArchivoEnvio, bucket_name, str_NombreArchivoEnS3)

        return

    def ObtenerQueries(self):

        queries = {}
        for sql_file in Path('sql').glob('*.sql'):
            with open(sql_file,'r') as sql:
                sql_key = sql_file.stem
                query = str(sql.read())
                queries[sql_key] = query

        return queries

    def InsertarEnRDSDesdeArchivo(self, conn, data_file, nombre_tabla):
        import io

        with conn.cursor() as cursor:
            print('nombre_tabla: ' + nombre_tabla)

            #Armamos la cadena sql concatenando el nombre de la tabla recibido como parámetro
            sql_statement = f"copy linaje." + nombre_tabla + " from stdin with csv delimiter as ','"
            print(sql_statement)
            buffer = io.StringIO()


            with open(data_file,'r') as data:
                buffer.write(data.read())
            buffer.seek(0)
            cursor.copy_expert(sql_statement, file=buffer)
