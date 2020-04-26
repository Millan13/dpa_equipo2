# Archivo que contiene la clase Eda, con todos los métodosa utilizar.
import pandas as pd
import numpy as np
import statistics as stats
# import pprint
# import matplotlib.pyplot as plt
# import seaborn as sns
# import statsmodels.api as sm
# import pandas_profiling

from sklearn import tree
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import chi2
# from sklearn.feature_selection import SelectFromModel
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_predict
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder
# from sklearn.preprocessing import MinMaxScaler
# from sklearn.preprocessing import QuantileTransformer



class Eda:

    # Declaración de propiedades

    # Propiedades Generales de un csv
    strRutaDataSource = ''
    pdDataSet = ''  # Vacío
    pdDataSetOriginal = ''  # Vacío
    nbrObservaciones = 0
    nbrVariables = 0
    strSeparadorColumnas = ';'

    # Propiedades respecto a las variables
    mtrxVariablesSeparables = np.array([['', '', '']])
    mtrxResumen_Var_Num = np.array([['']])
    mtrxResumen_Var_Cat = np.array([['']])
    pdMatrizResumen_Num = ''  # Vacío
    pdMatrizResumen_Cat = ''  # Vacío

    # Propiedades respecto a transformaciones
    listTransform = ['']
    pdTransfAux = ''  # Vacío
    npLabelEncoderFeat = np.array([])
    mtrxMapeoFeatOneHotEnc = np.array([['', '', '']])

    # Propiedades respecto a predicciones
    X_train = np.array([])
    Y_train = np.array([])

    X_test = np.array([])
    Y_test = np.array([])

    npBackSelFeatElim = np.array([])
    npBackSelFeatPers = np.array([])

    # Gets y Sets
    def __getnbrObservaciones(self):
        return self.__nbrObservaciones

    def __getnbrVariables(self):
        return self.__nbrVariables

    # Declaración de mètodos
    def __init__(self):
        strVar_Num1 = 'Variable'
        strVar_Num2 = 'Mínimo'
        strVar_Num3 = 'Máximo'
        strVar_Num4 = 'Promedio'
        strVar_Num5 = 'Varianza'
        strVar_Num6 = 'Desv. Est.'
        strVar_Num7 = 'qal25'
        strVar_Num8 = 'qal50'
        strVar_Num9 = 'qal75'
        self.mtrxResumen_Var_Num = np.array([[strVar_Num1,
                                              strVar_Num2,
                                              strVar_Num3,
                                              strVar_Num4,
                                              strVar_Num5,
                                              strVar_Num6,
                                              strVar_Num7,
                                              strVar_Num8,
                                              strVar_Num9]])

        strVar_Cat1 = 'Variable'
        strVar_Cat2 = 'Uniques'
        strVar_Cat3 = 'Mode'
        self.mtrxResumen_Var_Cat = np.array([[strVar_Cat1,
                                              strVar_Cat2,
                                              strVar_Cat3]])

        self.listTransform = ['']

        return

    def Cargar_Datos(self):
        self.pdDataSet = pd.read_csv(self.strRutaDataSource,
                                     sep=self.strSeparadorColumnas)
        self.nbrObservaciones = self.pdDataSet.shape[0]
        self.nbrVariables = self.pdDataSet.shape[1]
        self.pdDataSetOriginal = self.pdDataSet.copy()
        return

    def Limpiar_Datos(self):
        self.Convertir_A_Minusculas()
        self.Quitar_Acentos()
        self.Quitar_Caracteres_Invalidos()
        return

    def Transformar_Datos(self):
        self.Separar_Variables()
        return

    def Mostrar_Resumen(self):
        print('Fuente: ', self.strRutaDataSource)
        print('Observaciones: ', self.nbrObservaciones)
        print('Variables: ', self.nbrVariables)
        return

    # Métodos Auxiliares
    def Convertir_A_Minusculas(self):

        # Ponemos en minúsculas los títulos de las columnas
        self.pdDataSet.columns = self.pdDataSet.columns.str.lower()

        # Barremos las columnas tipo object
        for NombreColumna in self.pdDataSet.select_dtypes(include=[np.object]).columns:
            self.pdDataSet[NombreColumna] = self.pdDataSet[NombreColumna].str.lower()
        return

    def Quitar_Acentos(self):
        self.pdDataSet.columns = self.pdDataSet.columns.str.replace('á', 'a')
        self.pdDataSet.columns = self.pdDataSet.columns.str.replace('é', 'e')
        self.pdDataSet.columns = self.pdDataSet.columns.str.replace('í', 'i')
        self.pdDataSet.columns = self.pdDataSet.columns.str.replace('ó', 'o')
        self.pdDataSet.columns = self.pdDataSet.columns.str.replace('ú', 'u')
        return

    def Quitar_Caracteres_Invalidos(self):
        # Quitamos los caracteres extraños
        objCols = self.pdDataSet.select_dtypes(include=[np.object]).columns
        # self.pdDataSet[objCols] = self.pdDataSet[objCols].apply(lambda x: x.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(' ','_').str.replace('nan','NaN'))
        self.pdDataSet[objCols] = self.pdDataSet[objCols].apply(lambda x: x.astype(str).str.replace('á','a').
                    str.replace('é', 'e').
                    str.replace('í', 'i').
                    str.replace('ó', 'o').
                    str.replace('ú', 'u').
                    str.replace(' ', '_').
                    str.replace(',', '').
                    str.replace('.', '').
                    str.replace(';', '').
                    str.replace('+', '').
                    str.replace('nan', 'NaN'))

        self.Quitar_Acentos()
        self.pdDataSet.columns = self.pdDataSet.columns.str.replace(' ', '_')
        self.pdDataSet.columns = self.pdDataSet.columns.str.replace('ñ', 'n')
        return

    def Add_Variable_Separable(self, strNombreVariable, strCantidadSeparaciones, strSeparador):

        if self.mtrxVariablesSeparables.size == 3 and \
         self.mtrxVariablesSeparables[0][0] == '' and \
         self.mtrxVariablesSeparables[0][1] == '' and \
         self.mtrxVariablesSeparables[0][2] == '':
            self.mtrxVariablesSeparables = np.append(self.mtrxVariablesSeparables,[(strNombreVariable,strCantidadSeparaciones,strSeparador)],axis=0)
            self.mtrxVariablesSeparables = np.delete(self.mtrxVariablesSeparables,0,0)
        else:
            self.mtrxVariablesSeparables = np.append(self.mtrxVariablesSeparables,[(strNombreVariable,strCantidadSeparaciones,strSeparador)],axis=0)

        return

    def Separar_Variables(self):

        # Barremos nuestra lista de variables separables:
        for filaSeparable in self.mtrxVariablesSeparables:

            strNombreCol = filaSeparable[0]
            strCantidadSep = filaSeparable[1]
            strSeparador = filaSeparable[2]

            series = self.pdDataSet[strNombreCol].str.split(strSeparador).str

            for i in range(1, int(strCantidadSep)+1):
                self.pdDataSet[strNombreCol+str(i)] = series.get(i-1)

        return

    def Add_Variable_Resumen(self, strNombreVariable):
        self.mtrxResumen_Var_Num = np.append(self.mtrxResumen_Var_Num,[(strNombreVariable,'','','','','','','','')],axis=0)
        return

    def Add_Variable_Resumen_Cat(self, strNombreVariable):
        self.mtrxResumen_Var_Cat=np.append(self.mtrxResumen_Var_Cat,[(strNombreVariable,'','')],axis=0)
        return

    def Add_Variable_Resumen_Graph(self, strNombreVariable):
        self.grapf

    def Calcular_Estadisticas_Resumen(self):

        # ### Variables numéricas
        for fila in self.mtrxResumen_Var_Num[1:]:
            fila[1] = self.Calcular_Min(self.pdDataSet[fila[0]].values)
            fila[2] = self.Calcular_Max(self.pdDataSet[fila[0]].values)
            fila[3] = self.Calcular_Promedio(self.pdDataSet[fila[0]].values)
            fila[4] = self.Calcular_Varianza(self.pdDataSet[fila[0]].values)
            fila[5] = self.Calcular_Desv_Est(self.pdDataSet[fila[0]].values)
            fila[6] = self.Calcular_q25(self.pdDataSet[fila[0]].values)
            fila[7] = self.Calcular_q50(self.pdDataSet[fila[0]].values)
            fila[8] = self.Calcular_q75(self.pdDataSet[fila[0]].values)
        self.pdMatrizResumen_Num = pd.DataFrame(data=self.mtrxResumen_Var_Num[1:,1:], index=self.mtrxResumen_Var_Num[1:,0],columns=self.mtrxResumen_Var_Num[0,1:])

        # ### Variables categóricas
        for fila_cat in self.mtrxResumen_Var_Cat[1:]:
            fila_cat[1] = self.Calcular_NumCat(self.pdDataSet[fila_cat[0]].values)
            fila_cat[2] = self.Calcular_Mode(self.pdDataSet[fila_cat[0]].values)
        self.pdMatrizResumen_Cat = pd.DataFrame(data=self.mtrxResumen_Var_Cat[1:,1:], index=self.mtrxResumen_Var_Cat[1:,0],columns=self.mtrxResumen_Var_Cat[0,1:])

        return

    def Calcular_Min(self, npValores):
        nbrMinimo = np.nanmin(npValores)
        return nbrMinimo

    def Calcular_Max(self, npValores):
        nbrMaximo = np.nanmax(npValores)
        return nbrMaximo

    def Calcular_Promedio(self, npValores):
        nbrPromedio = np.nanmean(npValores)
        return nbrPromedio

    def Calcular_Varianza(self, npValores):
        nbrVar = np.nanvar(npValores)
        return nbrVar

    def Calcular_Desv_Est(self, npValores):
        nbrDesvEst = np.nanstd(npValores)
        return nbrDesvEst

    def Calcular_q25(self, npValores):
        nbrqal25 = np.nanquantile(npValores,.25)
        return nbrqal25

    def Calcular_q50(self, npValores):
        nbrqal50 = np.nanquantile(npValores,.50)
        return nbrqal50

    def Calcular_q75(self, npValores):
        nbrqal75 = np.nanquantile(npValores,.75)
        return nbrqal75

    def Calcular_NumCat(self, pdValores):
        nbrNumCat = np.unique(pdValores).size
        return nbrNumCat

    def Calcular_Mode(self, pdValores):
        nbrMode = stats.mode(pdValores)
        return nbrMode

    def Calcular_Boxplots(self, pdValores):
        nbrBoxPLot = sns.boxplot(x="alcaldia", y="consumo_total", data=pdDataSet)
        return nbrBoxPLot

    def Graficar_Plot_Barras(self, pdData, variable_ejex,variable_ejey,etiqueta_ejex,etiqueta_ejey,titulo):
        result = pdData.groupby([variable_ejex])[variable_ejey].mean().reset_index().sort_values(variable_ejey,ascending=False)
        graf = sns.barplot(x=variable_ejex, y=variable_ejey, data=pdData, order=result[variable_ejex])
        graf.set_xticklabels(graf.get_xticklabels(), rotation=40, ha="right")
        graf.set(xlabel=etiqueta_ejex, ylabel=etiqueta_ejey, title=titulo)
        return

    def Graficar_Plot_Barras2(self, pdData,variable_ejex,variable_ejey,lista_orden_barras,etiqueta_ejex,etiqueta_ejey,titulo):
        graf = sns.catplot(x=variable_ejex, y=variable_ejey,order=lista_orden_barras,
                kind="bar",data=pdData)
        graf.set(xlabel=etiqueta_ejex, ylabel=etiqueta_ejey, title=titulo)
        return

    def Graficar_Degradado(self, pdData,variable_ejex,variable_ejey,etiqueta_ejex,etiqueta_ejey,titulo):
        graf = sns.catplot(x=variable_ejex, y=variable_ejey,kind="strip", dodge=False, data=pdData, palette="pastel")
        graf.set(xlabel=etiqueta_ejex, ylabel=etiqueta_ejey, title=titulo)
        return

    def Graficar_Heat_Map(self, pdData):
        corr = pdData.corr()
        sns.heatmap(corr, mask=np.zeros_like(corr, dtype=np.bool), cmap=sns.diverging_palette(220, 10, as_cmap=True),
      square=True)
        return

    def Mostrar_Valores_Extremos(self, columna, nbrCandidad, strTipo):
        if strTipo == 'H':
            return pd.value_counts(columna).head(nbrCandidad)
        if strTipo == 'T':
            return pd.value_counts(columna).tail(nbrCandidad)
        return

    def Imprimir_Variables(self, strTipo):
        if strTipo == 'Cat':
            print('---------------- Variables Categóricas ----------------' )
            for NombreColumna in self.pdDataSet.select_dtypes(include=[np.object]).columns:
                print(NombreColumna)
        if strTipo == 'Num':
            print('---------------- Variables Numéricas ----------------' )
            for NombreColumna in self.pdDataSet.select_dtypes(include=[np.float, np.int]).columns:
                print(NombreColumna)
        return

    def Graficar_Plot_Dist(self, columna):
        sns.distplot(columna)
        return

    def Mostrar_Pct_Nan_X_Col(self):

        npArray = np.array([[0,0]])

        # Barremos las columnas para obtener los nombres
        for NombreColumna in self.pdDataSet.columns:

            # Obtenemos la cantidad de valores Nan x cada columna
            nbrNanXColumna=self.pdDataSet[NombreColumna].isnull().sum()

            srtQuery = NombreColumna + ' != ' + NombreColumna ### Para sacar los NaN de las categóricas
            nbrNanXColumna=self.pdDataSet.query(srtQuery).shape[0]

            # Calculamos el porcentaje
            nbrPctNanXColumna=nbrNanXColumna*100/self.nbrObservaciones

            # Guardamos en un array el nombre de la columna y el porcentaje
            npArray=np.append(npArray, [[NombreColumna, nbrPctNanXColumna]],axis=0)

        # Borramos el índice 0 (vacío)
        npArray=np.delete(npArray, 0, axis=0)

        # Mostramos los valores ordenados de menor a mayor
        print(npArray[npArray[:,1].astype(float).argsort()])

        return

    def Agregar_Features_Transform(self, strTipoValor, strNombreColumna):
        if self.listTransform == ['']:
            self.listTransform = [('impute_'+strNombreColumna, SimpleImputer(strategy=strTipoValor), [strNombreColumna])]
        else:
            self.listTransform.append(('impute_'+strNombreColumna, SimpleImputer(strategy=strTipoValor), [strNombreColumna]))
        return

    def Imputar_Features(self, objDatos):

        col_trans = ColumnTransformer(self.listTransform, remainder="passthrough", n_jobs=1)
        col_trans.fit(objDatos)
        objTransform = col_trans.transform(objDatos)
        # npTransfAux = col_trans.transform(self.pdDataSet)
        # self.pdDataSet = pd.DataFrame(npTransfAux)
        # self.pdDataSet.columns=columns
        return objTransform

    def Agregar_Features_LabelEnc(self, strNombreVar):
        # if self.npLabelEncoderFeat == np.array(['']):
        #    self.npLabelEncoderFeat = np.array([strNombreVar])
        # else:
        #    self.npLabelEncoderFeat = np.append(self.npLabelEncoderFeat, strNombreVar)
        self.npLabelEncoderFeat = np.append(self.npLabelEncoderFeat, strNombreVar)
        return

    def Borrar_Cols_Inter_LabelEnc(self):
        # Borramos las columnas resultado del label_encoder
        npArrayColumnas=np.array([])
        for elemento in self.npLabelEncoderFeat:
            npArrayColumnas=np.append(npArrayColumnas, elemento+'_encoded')

        self.pdDataSet = self.pdDataSet.drop(npArrayColumnas, axis=1)

        return

    def Borrar_Cols_Base_LabelEnc(self):
        # Borramos las columnas resultado del label_encoder
        npArrayColumnas=np.array([])
        for elemento in self.npLabelEncoderFeat:
            npArrayColumnas=np.append(npArrayColumnas, elemento)

        self.pdDataSet = self.pdDataSet.drop(npArrayColumnas, axis=1)

        return

    def Borrar_Cols_Expan_LabelEnc(self):
        npArrayColumnas = np.array([])
        for elemento in self.npLabelEncoderFeat:
            OneHotEnc = OneHotEncoder(categories = "auto")
            X1 = OneHotEnc.fit_transform(self.pdDataSet[elemento].values.reshape(-1,1)).toarray()

            for indice in range(X1.shape[1]):
                npArrayColumnas = np.append(npArrayColumnas, elemento+'_'+str(indice))

        self.pdDataSet = self.pdDataSet.drop(npArrayColumnas, axis=1)

        return

    def LabelEncoder_OneHotEncoder(self):

        labEnc = LabelEncoder()
        mtrxMapeoFeatOneHotEnc=np.array([['','','']])
        for feature in self.npLabelEncoderFeat:

            self.pdDataSet[feature+'_encoded'] = labEnc.fit_transform(self.pdDataSet[feature])

            # OneHotEncoder Pt1
            OneHotEnc = OneHotEncoder(categories = "auto")
            X1 = OneHotEnc.fit_transform(self.pdDataSet[feature+'_encoded'].values.reshape(-1,1)).toarray()

            # OneHotEncoder Pt2
            dfOneHot1 = pd.DataFrame(X1, columns = [feature+'_'+str(int(i)) for i in range(X1.shape[1])])
            self.pdDataSet = pd.concat([self.pdDataSet, dfOneHot1], axis=1)

            # mtrxMapeoFeatOneHotEnc
            aux=self.pdDataSet[feature].unique()
            aux.sort()

            for i, elem in enumerate(aux):
                self.mtrxMapeoFeatOneHotEnc=np.append(self.mtrxMapeoFeatOneHotEnc,[[feature, i, elem]], axis=0)

        # Borramos el primer elemento (vacío)
        self.mtrxMapeoFeatOneHotEnc=np.delete(self.mtrxMapeoFeatOneHotEnc,0,0)

        return

    def Generar_Train_Test(self, npX, npY, nbrTamanio):
        # Separamos nuestros datos en entrenamiento y prueba
        self.X_train, self.X_test, self.Y_train, self.Y_test = train_test_split(npX, npY, test_size = nbrTamanio, random_state = 0)
        return

    def Escalar_Train_Test(self):
        # Preparamos los datos numéricos para el modelado
        scaler = StandardScaler()
        self.X_train[:] = scaler.fit_transform(self.X_train[:])
        self.X_test[:] = scaler.transform(self.X_test[:])
        return

    def Aplicar_OLS(self,npX,npY):
        return

    def Mostrar_SummaryOLS(self, X_train, Y_train):
        regressor = sm.OLS(Y_train, X_train).fit()
        print(regressor.summary())
        return

    def Mostrar_LR_Score(self):
        # Realizamos nuestra primer predicción con todas las variables
        model = LinearRegression()
        model.fit(self.X_train,self.Y_train)
        print(model.score(self.X_test,self.Y_test))
        return

    def Agregar_Intercepto(self, objDatos):
        # self.X_train = np.append (arr=np.ones([objDatos.shape[0],1]).astype(float), values = objDatos, axis = 1)
        objResultado = np.append (arr=np.ones([objDatos.shape[0],1]).astype(float), values = objDatos, axis = 1)
        return objResultado

    def Backward_Selection(self, pValue=.05):

        # Creamos un numpy array con elementos enumerados como la cantidad de features con las que se quedó nuestro modelo
        X_opt = np.arange(self.X_train.shape[1])

        # Definimos el valor límite del pvalue
        nbrLimite=pValue

        # print('Antes de empezar el proceso de eliminar las features menos significativas')
        # print(X_opt)

        regressor = sm.OLS(self.Y_train, self.X_train[:,X_opt]).fit()
        # print(regressor.summary())

        npArrIndices=np.array([0])

        # Mientras el valor máximo del pvalue sea mayor al límite
        while (regressor.pvalues.max() > nbrLimite):

            # print()
            # print('regressor.pvalues.max():\n',regressor.pvalues.max())
            # print('regressor.pvalues.argmax():\n',regressor.pvalues.argmax())

            # Se elimina el índice que contiene el pvalue que está encima de nuestro límite
            X_opt = np.delete(X_opt, [regressor.pvalues.argmax()], axis=0)

            # Vamos almacenando los índices donde detectamos p-values que no cumplen con el límite
            npArrIndices = np.append(npArrIndices, regressor.pvalues.argmax())

            # Volvemos a correr la regresión sin la feature del mayor pvalue
            regressor = sm.OLS(self.Y_train, self.X_train[:,X_opt]).fit()

        # print(regressor.summary())

        # Borramos del arreglo, el elemento 0, (se agrega por defecto)
        npArrIndices = np.delete(npArrIndices,0)

        npColumnasCompletas = np.arange(self.X_train.shape[1])
        npColumnasReducidas = npColumnasCompletas
        npColumnasEliminadas = np.array([])
        npEliminados = npArrIndices

        # print()
        # print('Columnas completas:\n', npColumnasCompletas)
        # print('Índices a eliminar:\n', npEliminados)

        for elemento in npEliminados:
            npColumnasEliminadas = np.append(npColumnasEliminadas, npColumnasReducidas[elemento])
            npColumnasReducidas = np.delete(npColumnasReducidas,elemento)

        # print('Columnas eliminadas:\n',npColumnasEliminadas)
        # print('Columnas Reducidas:\n',npColumnasReducidas)

        npArrIndices = npColumnasEliminadas

        npArrIndices = npArrIndices.astype(int)

        return npArrIndices

    def Calcular_Chi_Cuadrada(self, X, Y):
        chi_scores = chi2(X, Y)
        p_values = pd.Series(chi_scores[1], index=X.columns)
        p_values.sort_values(ascending=False, inplace=True)

        print('P_values:\n', p_values)
        p_values.plot.bar()

        return

    def SepararFeaturesYPred(self, strPred):

        # Separamos nuestros datos en X y Y (características y resultados)

        # Obtenemos todo el data frame y quitamos la variable que queremos predecir
        pdX = self.pdDataSet
        pdX = pdX.drop(strPred,axis=1)

        # Obtenemos sólo la variable que queremos predecir
        pdY = self.pdDataSet[strPred].to_frame()

        return pdX, pdY

    def prepModelos(self, npModelos):

        npArrayModelos = np.array([])
        for strModelo in npModelos:

            if strModelo == 'DECTREE':
                classifier = tree.DecisionTreeClassifier()
            if strModelo == 'RANDOMF':
                classifier = RandomForestClassifier()
            if strModelo == 'XGBOOST':
                classifier = GradientBoostingClassifier()

            npArrayModelos = np.append(npArrayModelos, classifier)

        return npArrayModelos

    def magic_loop2(self, npClassifier, npDictHyperParams, X_train, Y_train, nbrCv):

        npResultados = np.array([])
        for i, classifier in enumerate(npClassifier):
            dictHyperParams = npDictHyperParams[i]

            grid_search = GridSearchCV(classifier,
                                       dictHyperParams,
                                       scoring='f1',
                                       cv=nbrCv,
                                       n_jobs=-1,
                                       verbose=3
                                       )
            grid_search.fit(X_train, Y_train)
            npResultados = np.append(npResultados, grid_search)

            # de los valores posibles que pusimos en el grid, cuáles fueron los mejores
            print('grid_search.best_params_: ', grid_search.best_params_)

            # mejor score asociado a los modelos generados con los diferentes hiperparametros
            # corresponde al promedio de los scores generados con los cv
            print('grid_search.best_score_: ', grid_search.best_score_)


        return npResultados

    def InstanciarModeloDinamico(self, npModelos, nbrIndice, dictParametros):

        strModelo = npModelos[nbrIndice]

        if strModelo == 'DECTREE':
            classifier = tree.DecisionTreeClassifier(**dictParametros)
        if strModelo == 'RANDOMF':
            classifier = RandomForestClassifier(**dictParametros)
        if strModelo == 'XGBOOST':
            classifier = GradientBoostingClassifier(**dictParametros)

        return classifier
