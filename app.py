from flask import Flask, json
from flask_sqlalchemy import SQLAlchemy
import pandas as pd

# inicializar banco de dados em memória
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Carregar dados do CSV para o banco de dados
df_filmes = pd.read_csv('movielist.csv', sep=';')

# Preencher valores ausentes na coluna 'winner' com False
df_filmes['winner'] = df_filmes['winner'].map({'yes': True, 'no': False}).fillna(False)


# Classe filme para definir os atributos do banco de dados
class Filme(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer)
    title = db.Column(db.String(255))
    studios = db.Column(db.String(255))
    producers = db.Column(db.String(255))
    winner = db.Column(db.Boolean)


# Crie a tabela no banco de dados
with app.app_context():
    db.create_all()

    # Preencher a tabela com os dados do CSV
    for indice, linha in df_filmes.iterrows():
        filme = Filme(year=linha['year'], title=linha['title'], studios=linha['studios'], producers=linha['producers'], winner=linha['winner'])

        db.session.add(filme)

    db.session.commit()

# Rotas da API
@app.route('/producers', methods=['GET'])
def get_producers():
    with app.app_context():
        # Lógica para obter os produtores com maior intervalo e mais rápidos
        # Utilização da biblioteca Pandas para manipulação de dados.

        # Utiliza o DataFrame df_filmes carregado anteriormente
        df = df_filmes.copy()

        # Ordena o DataFrame pela coluna 'year'
        df.sort_values(by='year', inplace=True)

        # Inicializa as variáveis para armazenar os resultados
        lista_intervalo = []

        # Agrupa o DataFrame pela coluna 'producers'
        producers_agrupados = df.groupby('producers')

        # Itera sobre os grupos de produtores
        for producer, group in producers_agrupados:
            # Calcula os intervalos entre os prêmios consecutivos
            intervalos = group['year'].diff().fillna(0)

            # Se o tamanho do intervalo for maior que 1 e se o winner for igual a true
            if len(intervalos) > 1 and group['winner'].all():
                min_index = intervalos.idxmin()
                max_index = intervalos.idxmax()

                # Obtém as informações para o mínimo intervalo
                infos = {
                    'producer': producer,
                    'interval': int(intervalos[max_index]),
                    'previousWin': int(df.at[min_index, 'year']),
                    'followingWin': int(df.at[max_index, 'year'])
                }
                lista_intervalo.append(infos)

        # Ordena a lista por 'interval'
        lista_intervalo = sorted(lista_intervalo, key=lambda x: x['interval'])

        # Divide a lista ao meio para criar os dicionários de min e max
        metade_tamanho = len(lista_intervalo) // 2
        resultado_json = {'min': lista_intervalo[:metade_tamanho], 'max': lista_intervalo[metade_tamanho:]}

        # Retorna a resposta da API
        return app.response_class(
            response=json.dumps(resultado_json, indent=2, sort_keys=False),
            status=200,
            mimetype='application/json'
        )


if __name__ == '__main__':
    app.run(debug=True)
