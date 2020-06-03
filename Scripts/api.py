from flask import Flask
from flask_restplus import Api, Resource
from flask import request
import pandas as pd
from Class_Utileria import Utileria

from Class_Rita import Rita

app = Flask(__name__)
api = Api(app)
ns = api.namespace('Api-de-Rita',
                   description='Predicciones de retrasos sobre vuelos')


@ns.route('/info_modelo', endpoint='endpoint_info_modelo')
class Modelo(Resource):
    """Aquí debe ir la descripción de la clase Modelo
    Aquí debe ir el segundo renglón
    Y aquí debe ir el tercer renglón
    """

    def get(self):
        obj_Rita = Rita()
        return {'Campos del modelo': obj_Rita.dict_Campos}


@ns.route("/predict")
class Predict(Resource):

    obj_Rita = Rita()
    meses = obj_Rita.ObtenerMeses()

    parser = ns.parser()
    parser.add_argument('fecha', type=str, help='Fecha del vuelo', nullable=True)
    parser.add_argument('id_operador', type=str, help='Id aerolinea', nullable=True)
    parser.add_argument('id_avion', type=str, help='Id avion', nullable=True)
    parser.add_argument('num_vuelo', type=str, help='No. vuelo', nullable=True)
    parser.add_argument('origen', type=str, help='Origen del vuelo', nullable=True)
    parser.add_argument('destino', type=str, help='Destino del vuelo', nullable=True)
    parser.add_argument('horasalidaf', type=str, help='Salida vuelo', nullable=True)
    @ns.expect(parser, validate=True)
    def get(self):

        fecha = request.args.get('fecha')
        id_operador = request.args.get('id_operador')
        id_avion = request.args.get('id_avion')
        num_vuelo = request.args.get('num_vuelo')
        origen = request.args.get('origen')
        destino = request.args.get('destino')
        horasalidaf = request.args.get('horasalidaf')

        from Class_Utileria import Utileria
        obj_Utileria = Utileria()


        str_Query="select * from trabajo.predicciones where fecha = '2016-01-02' and id_operador = 'WN' and id_avion='7819A' and num_vuelo='6308' and origen='MDW' and destino = 'PIT' and horasalidaf='545'";
        str_Query="select * from trabajo.predicciones where 1=1"

        if fecha is not  None:
            str_Query=str_Query + " and fecha = '{}'".format(fecha)
        if id_operador is not  None:
            str_Query=str_Query + " and id_operador = '{}'".format(id_operador)
        if id_avion is not  None:
            str_Query=str_Query + " and id_avion = '{}'".format(id_avion)
        if num_vuelo is not  None:
            str_Query=str_Query + " and num_vuelo = '{}'".format(num_vuelo)
        if origen is not  None:
            str_Query=str_Query + " and origen = '{}'".format(origen)
        if destino is not  None:
            str_Query=str_Query + " and destino = '{}'".format(destino)
        if horasalidaf is not None:
            str_Query=str_Query + " and horasalidaf = '{}'".format(horasalidaf)

        str_Query=str_Query + ";"

        try:
            connection = Utileria().CrearConexionRDS()
            cursor = connection.cursor()

            cursor.execute(str_Query)

            records = cursor.fetchall()
            df = pd.DataFrame(records)
            col_names =  [i[0] for i in cursor.description]
            df.columns = col_names
            cursor.close()
            connection.close()

        except:
            raise

        return df.to_dict()


if __name__ == '__main__':

    app.run(host='0.0.0.0', debug = False)
    #app.run()
