#Bias & Fairness
### Carga modelo y preparación de tabla aequitas
import numpy as np
import pickle as picklea
import pandas as pd
from Class_Eda import Eda
from sklearn.base import BaseEstimator, ClassifierMixin
import aequitas
import seaborn as sns
from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
from aequitas.plotting import Plot
from aequitas.preprocessing import preprocess_input_df
from aequitas.group import Group

#Instanciamos el objeto Eda
objEda = Eda()
#Inicializamos los parámetros principales (por el momento, sólo es uno: la ruta de la fuente de datos)
objEda.strRutaDataSource='Transit_modeling.csv' #El archivo que sale del feature engineering
#Proceso de carga
objEda.Cargar_Datos()
#Proceso de limpieza
objEda.Limpiar_Datos()
#Guardamos el arreglo en la nueva columna
objEda.pdDataSet['y'] = objEda.pdDataSet.apply(lambda x: (x.etiqueta1), axis=1)

################## Separamos las features de lo que vamos a predecir
pdX, pdY = objEda.SepararFeaturesYPred('y')

################## Separamos nuestros datos en entrenamiento y pruebas utilizando la proporción 80-20
objEda.Generar_Train_Test(pdX, pdY, 0.2)

################## Preparamos las variables que imputaremos
objEda.listTransform=[''] #Limpiamos la propiedad de lista de features a imputar
objEda.Agregar_Features_Transform('median', 'vuelos_afectados') #no hizo nada porque están como NaN

################## Imputamos sobre el conjunto de entrenamiento y prueba
objEda.X_train = objEda.Imputar_Features(objEda.X_train)
objEda.X_test = objEda.Imputar_Features(objEda.X_test)

#Convertir el numpy a dataframe
X_test_df = pd.DataFrame(objEda.X_test)

#Dejar el día de la semana
X_test_df = X_test_df[2]

#Cambiar el nombre de la columnas para Aquitas
labels_train_df = ['day_sem']
labels_test_df = labels_train_df

X_test_df = pd.DataFrame(X_test_df) #Se convierte en numpy array en Pandas
X_test_df.columns=labels_train_df

#Importamos modelo
pickleName = 'ModeloFinalRita.p'
pickleFile = open(pickleName, 'rb')
model = pickle.load(pickleFile)
pickleFile.close()
model

# También importamos los conjuntos de prueba utilizados para Rita

pickleName = 'X_testRita.p'
pickleFile = open(pickleName, 'rb')
X_test = pickle.load(pickleFile)
pickleFile.close()

pickleName = 'Y_testRita.p'
pickleFile = open(pickleName, 'rb')
Y_test = pickle.load(pickleFile)
pickleFile.close()

#Hacemos el fit
model.fit(X_train, Y_train)

#Función que realiza las predicciones
predict_model=lambda x: model.predict_proba(x).astype(float)

#predict_fn_model = lambda x: model.predict_proba(x).astype(float)
predict_model

#Cambiar el nombre de la columnas para Aquitas
labels_train = ['count','max','nvue_falt','vuelos_afectados','lunes','martes','miercoles','jueves','viernes','sabado','domingo']
labels_test = labels_train

X_test = pd.DataFrame(X_test) #Se convierte en numpy array en Pandas
X_test.columns=labels_train
X_test.head()

#Renombramos las variables según la estructura el input data
predicciones=pd.DataFrame(model.predict(X_test))
predicciones=predicciones.rename(columns={0: "score"})

#Modificaciones a Y_test
#y_test=pd.DataFrame(Y_test)
y_test=Y_test.rename(columns={"y": "label_value"})
y_test.reset_index(drop=True, inplace=True)
y_test.shape

#Unimos los dataframe para generar el input data
datos_aequitas=pd.concat([predicciones,y_test,X_test,X_test_df], axis=1)
#Hasta aquí es la preparación de datos para aequitas ------------------------------------------------------------

#Filtrar los datos de aequitas para calculo de FNR
datos_aequitas = datos_aequitas[['score','label_value','day_sem']]
datos_aequitas.head()

#Instalación de Aequitas
#pip install aequitas

g = Group()
xtab, _ = g.get_crosstabs(datos_aequitas)
#xtab contiene calculos de todas la métricas de FP, FN, TP, TN

#Calculo de Bias
b = Bias()
bdf = b.get_disparity_predefined_groups(xtab,
                    original_df=datos_aequitas,
                    ref_groups_dict={'day_sem':'e:viernes'},
                    alpha=0.05,
                    check_significance=False)


#Calculo de Fairness
f = Fairness()
fdf = f.get_group_value_fairness(bdf) #Mismo grupo de referencia

#Exportar archivos
ruta = "bdf.csv"
bdf.to_csv(ruta)

ruta = "fdf.csv"
fdf.to_csv(ruta)
