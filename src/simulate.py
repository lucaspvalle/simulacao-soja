import pandas as pd
from datetime import datetime, timedelta

from src.utils import cronometro, logging
from src.monte_carlo import best_fit_distribution


@cronometro
def get_clima_por_hora(cnx, rota_id, respeita_turno=True):
    """
    Analisa o itinerário de cada rota para inferir o horário em que o veículo passará em cada cidade
    Com isso, podemos cruzar com os dados metereológicos para saber as condições climáticas históricas no local

    :param cnx: conexão com o banco de dados local
    :param rota_id: identificador da rota de movimentação de soja (por ex: Barreiras -> Cuiabá)
    :param respeita_turno: restringe a viagem de veículos apenas a horários comerciais
    """
    logging.info('Analisando itinerário de cada rota!')

    _, origem_rota, destino_rota, primeiro_dia = \
        cnx.execute(f"select * from rotas where rota_id = {rota_id}").fetchone()
    itinerario = pd.read_sql(f"select * from transit_time where rota_id = {rota_id}", cnx).assign(inicio=None, fim=None)

    # horizonte de planejamento:
    primeiro_dia = datetime.strptime(primeiro_dia + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    ultimo_dia = primeiro_dia + timedelta(minutes=int(itinerario['transit_time'].sum()), days=1)

    dm_query = (f"select c.cidade_id, c.cidade, dm.* from dados_metereologicos dm "
                f"inner join cidades c using (estacao_id) "
                f"where dm.mes between {primeiro_dia.month} and {ultimo_dia.month} "
                f"  and dm.dia between {primeiro_dia.day} and {ultimo_dia.day}")

    dm = pd.read_sql(dm_query, cnx, parse_dates=['timestamp'])

    # inicializa possíveis horários de saída em horários comerciais (exclui horário de almoço):
    # TODO: ver com caminhoneiros que horários são esses
    horario_de_trabalho = [hora for hora in range(0, 24, 1) if (8 <= hora <= 18 and hora != 12) or not respeita_turno]
    saidas = [primeiro_dia + timedelta(hours=hora) for hora in horario_de_trabalho]

    for horario in saidas:
        logging.info(f'(Rota: {rota_id}) Saida: {horario}')
        horario_inicial = horario

        origem = origem_rota
        destino = None

        while destino != destino_rota:
            subset = itinerario.query(f'origem == {origem}')

            destino = subset['destino'].values[0]
            transito = subset['transit_time'].values[0]

            horario_final = horario_inicial + timedelta(minutes=int(transito))

            # tratamento: se não for horário comercial, joga para o primeiro horário de trabalho disponível
            if horario_final.hour not in horario_de_trabalho:
                if horario_final.hour > 18:
                    horario_final += timedelta(hours=32 - horario_final.hour, minutes=horario_final.minute * -1)

                elif horario_final.hour < 8:
                    horario_final += timedelta(hours=8 - horario_final.hour, minutes=horario_final.minute * -1)

                elif horario_final.hour == 12:
                    horario_final += timedelta(minutes=60 - horario_final.minute)

            itinerario.loc[itinerario['origem'] == origem, 'inicio'] = horario_inicial
            itinerario.loc[itinerario['origem'] == origem, 'fim'] = horario_final

            horario_inicial = horario_final
            origem = destino

        itinerario['inicio'] = pd.to_datetime(itinerario['inicio'])
        itinerario['fim'] = pd.to_datetime(itinerario['fim'])

        dm['hora'] = dm['timestamp'].dt.time

        data = (itinerario
                .merge(dm, left_on=['origem'], right_on=['cidade_id'])
                # não podemos filtrar pelo timestamp porque queremos dados de anos passados
                .query('hora >= inicio.dt.time and hora <= fim.dt.time '
                       'and mes >= inicio.dt.month and mes <= fim.dt.month '
                       'and dia >= inicio.dt.day and dia <= fim.dt.day')
                .assign(saida=horario)
                )[['cidade_id', 'cidade', 'saida', 'mes', 'dia', 'hora', 'temperatura', 't_max', 't_min',
                   'umidade', 'u_max', 'u_min']]

        simulate_distribuicoes(cnx, data)


@cronometro
def calculate_distribuicoes(cnx, dataset, medida):
    if medida == "temperatura":
        cols = ['temperatura', 't_max', 't_min']
    else:
        cols = ['umidade', 'u_max', 'u_min']

    # o banco de dados do INMET disponibiliza dados atuais, máximos e mínimos
    # utilizamos ambos como ocorrências de temperaturas em determinada hora para aumentar o conjunto de dados
    df = (pd.melt(dataset, id_vars=['saida', 'cidade_id', 'cidade', 'mes', 'dia', 'hora'], value_vars=cols,
                  value_name='value')
          .drop(columns=['variable'])
          .dropna(subset=['value']))

    dist_por_hora = []

    for index, values in df.groupby(['saida', 'cidade', 'mes', 'dia', 'hora']):
        cidade_id = values['cidade_id'].iloc[0]
        cidade = values['cidade'].iloc[0]
        saida = values['saida'].iloc[0]
        mes = str(values['mes'].iloc[0]).zfill(2)  # convertendo mês 6 para 06 (mês 10 continua 10)
        dia = str(values['dia'].iloc[0]).zfill(2)  # convertendo dia 9 para 09 (dia 10 continua 10)
        hora = values['hora'].iloc[0]

        logging.info(f"Avaliando {medida}: {cidade} ({dia}/{mes} {hora})")

        dist_name, params = best_fit_distribution(values)
        dist_por_hora.append((str(saida), cidade_id, mes, dia, str(hora), medida, dist_name, str(params)))

    (pd.DataFrame(dist_por_hora, columns=['saida', 'cidade_id', 'mes', 'dia', 'hora', 'medida', 'dist_name', 'params'])
     .to_sql('distribuicoes', cnx, if_exists='append', index=False))


@cronometro
def simulate_distribuicoes(cnx, data):
    """
    Infere a distribuição de probabilidade que melhor representa as condições climáticas de cada local da rota
    em determinado horário

    :param cnx: conexão com o banco de dados local
    :param data: conjunto de dados com a data (dia e hora) em que o veículo estará em cada cidade
    """
    saida = str(data['saida'].iloc[0])

    cnx.execute(f"delete from distribuicoes where saida = '{saida}'")
    cnx.commit()

    for medida in ['temperatura', 'umidade']:
        calculate_distribuicoes(cnx, data, medida)


@cronometro
def simulate(cnx, rota_id, respeita_turno):
    get_clima_por_hora(cnx, rota_id, respeita_turno)
