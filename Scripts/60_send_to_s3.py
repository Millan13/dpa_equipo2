import boto3

s3=boto3.client(
        's3',
	    aws_access_key_id='USUARIO',
        aws_secret_access_key='CONTRASEÑA',
        region_name='us-west-2', #Oregon
        use_ssl=False
    )

bucketName = "bucket-rita"
Key = "Datos/all.csv"
outPutname = "carga_inicial/all.csv"

s3.upload_file(Key,bucketName,outPutname)
