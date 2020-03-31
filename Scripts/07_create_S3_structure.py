import boto3
from RitaWebScrap import Auxiliar

print('\n---Inicio creación directorios---')

# Instanciamos la clase Auxiliar
objAuxiliar = Auxiliar()

arr_Anios=objAuxiliar.ObtenerAnios()
arr_Meses=objAuxiliar.ObtenerMeses()

cnx_S3=objAuxiliar.CrearConexionS3()
bucket_name = "bucket-rita"

for anio in arr_Anios:
    print('anio: ', anio)
    for mes in arr_Meses:
        print('mes: ', mes)

        # Directorios de la carga inicial
        directory_name = 'carga_inicial/'+str(anio)+'/'+str(mes)
        print('directory_name: ', directory_name)
        cnx_S3.put_object(Bucket=bucket_name, Key=(directory_name+'/'))

# Directorios de la carga recurrente
directory_name = 'carga_recurrente'
print('directory_name: ', directory_name)
cnx_S3.put_object(Bucket=bucket_name, Key=(directory_name+'/'))

print('---Fin creación directorios---\n')
