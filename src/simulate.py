import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

from src.utils import cronometro, logging

from src.fuzzy import get_fuzzy_results
from src.monte_carlo import DIST_x_FUNC, best_fit_distribution


@cronometro
def get_clima_por_hora(cnx, rota_id):
    """
    Analisa o itinerário de cada rota para inferir o horário em que o veículo passará em cada cidade
    Com isso, podemos cruzar com os dados meteorológicos para saber as condições climáticas históricas no local

    :param cnx: conexão com o banco de dados local
    :param rota_id: identificador da rota de movimentação de soja (por ex: Barreiras -> Cuiabá)
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

    # inicializa possíveis horários de saída em horários comerciais:
    saidas = [primeiro_dia + timedelta(hours=hora) for hora in range(6, 18, 1)]

    for horario in saidas:
        logging.info(f'(Rota: {rota_id}) Saida: {horario}')
        horario_inicial = horario

        origem = origem_rota
        destino = None

        horas_de_viagem, dias_de_viagem = 0, 0

        while destino != destino_rota:
            subset = itinerario.query(f'origem == {origem}')

            destino = subset['destino'].values[0]
            transito = subset['transit_time'].values[0]

            horario_final = horario_inicial + timedelta(minutes=int(transito))

            horas_de_viagem += transito
            dias_de_viagem += transito

            # caminhoneiros são obrigados legalmente a parar 30 minutos a cada 5h30 de viagem
            if horas_de_viagem >= (5 * 60 + 30):
                horario_final += timedelta(minutes=30)
                horas_de_viagem = 0
                dias_de_viagem += 30

            # caminhoneiros são obrigados legalmente a parar por 8 horas ininterruptas depois de 24h de viagem
            if dias_de_viagem >= (24 * 60):
                horario_final += timedelta(hours=8)
                horas_de_viagem = 0
                dias_de_viagem = 0

            itinerario.loc[itinerario['origem'] == origem, 'inicio'] = horario_inicial
            itinerario.loc[itinerario['origem'] == origem, 'fim'] = horario_final

            horario_inicial = horario_final
            origem = destino

        itinerario['inicio'] = pd.to_datetime(itinerario['inicio'])
        itinerario['fim'] = pd.to_datetime(itinerario['fim'])

        dm['hora'] = dm['timestamp'].dt.time

        # (itinerario.assign(saida=horario)[['rota_id', 'saida', 'origem', 'destino', 'inicio', 'fim']]
        #  .to_sql('itinerario', cnx, if_exists='append', index=False))

        data = (itinerario
                .merge(dm, left_on=['origem'], right_on=['cidade_id'])
                # não podemos filtrar pelo timestamp porque queremos dados de anos passados
                .query('hora >= inicio.dt.time and hora <= fim.dt.time '
                       'and mes >= inicio.dt.month and mes <= fim.dt.month '
                       'and dia >= inicio.dt.day and dia <= fim.dt.day')
                .assign(saida=horario)
                )[['cidade_id', 'cidade', 'saida', 'mes', 'dia', 'hora', 'temperatura', 't_max', 't_min',
                   'umidade', 'u_max', 'u_min']]

        get_distribuicoes(cnx, rota_id, data)


@cronometro
def get_distribuicoes(cnx, rota_id, data):
    """
    Infere a distribuição de probabilidade que melhor representa as condições climáticas de cada local da rota
    em determinado horário

    :param cnx: conexão com o banco de dados local
    :param rota_id: identificador da rota de movimentação de soja (por ex: Barreiras -> Cuiabá)
    :param data: conjunto de dados com a data (dia e hora) em que o veículo estará em cada cidade
    """
    saida = str(data['saida'].iloc[0])

    cnx.execute(f"delete from distribuicoes where rota_id = {rota_id} and saida = '{saida}'")
    cnx.commit()

    for medida in ['temperatura', 'umidade']:
        if medida == "temperatura":
            cols = ['temperatura', 't_max', 't_min']
        else:
            cols = ['umidade', 'u_max', 'u_min']

        # o banco de dados do INMET disponibiliza dados atuais, máximos e mínimos
        # utilizamos ambos como ocorrências de temperaturas em determinada hora para aumentar o conjunto de dados
        df = (pd.melt(data, id_vars=['saida', 'cidade_id', 'cidade', 'mes', 'dia', 'hora'], value_vars=cols,
                      value_name='value')
              .drop(columns=['variable'])
              .dropna(subset=['value']))

        dist_por_hora = []

        for index, values in df.groupby(['saida', 'cidade', 'mes', 'dia', 'hora']):
            saida, cidade, mes, dia, hora = index

            mes = str(mes).zfill(2)
            dia = str(dia).zfill(2)
            cidade_id = values['cidade_id'].iloc[0]

            logging.info(f"Avaliando {medida}: {cidade} ({dia}/{mes} {hora})")

            dist_name, params = best_fit_distribution(values['value'])
            dist_por_hora += [(rota_id, str(saida), cidade_id, mes, dia, str(hora), medida, dist_name, params)]

        (pd.DataFrame(dist_por_hora,
                      columns=['rota_id', 'saida', 'cidade_id', 'mes', 'dia', 'hora', 'medida', 'dist_name', 'params'])
         .to_sql('distribuicoes', cnx, if_exists='append', index=False))


@cronometro
def simulate_por_hora(cnx, rota_id):
    """
    A partir das distribuições de probabilidade de condições climáticas, calculadas anteriormente
    para os pontos esperados em que o veículo esteja durante a rota, podemos simular diversos cenários
    por meio da geração de números aleatórios que repliquem a realidade.

    :param cnx: conexão com o banco de dados local
    :param rota_id: identificador da rota de movimentação de soja (por ex: Barreiras -> Cuiabá)
    """
    distribuicoes = pd.read_sql(f'select * from distribuicoes where rota_id = {rota_id}', cnx)
    distribuicoes['params'] = distribuicoes['params'].apply(lambda row: [float(item) for item in row.split(',')])

    cnx.execute(f'delete from resultados where rota_id = {rota_id}')
    cnx.execute(f'delete from simulacoes where rota_id = {rota_id}')
    cnx.commit()

    for index, values in distribuicoes.groupby(['saida', 'cidade_id', 'mes', 'dia', 'hora']):
        saida, cidade_id, mes, dia, hora = index

        hora = str(hora)
        mes = str(mes).zfill(2)
        dia = str(dia).zfill(2)

        logging.info(f"Ajustando distribuição: {saida} - {dia}/{mes} {hora}")

        simulated_df = pd.DataFrame()
        for medida in ['temperatura', 'umidade']:
            dist_name = values.loc[values['medida'] == medida, 'dist_name'].iloc[0]
            params = values.loc[values['medida'] == medida, 'params'].iloc[0]

            simulated_df[medida] = \
                DIST_x_FUNC.get(
                    dist_name,
                    lambda *_: logging.info(f"Falha no método de transformação de {dist_name}")  # tratamento de erro
                )(*params)                                                                       # chama a função

        simulated_df['saida'] = saida

        # exportando resultados agregados apenas em nível de rota, horário de saída e cenário (1..1000)
        (simulated_df.assign(rota_id=rota_id, cenario=simulated_df.index + 1)  # noqa
        [['rota_id', 'saida', 'cenario', 'temperatura', 'umidade']]
        .to_sql('simulacoes', cnx, if_exists='append', index=False))


@cronometro
# def get_results(cnx, rota_id):
#     simulated_df = pd.read_sql(f'select * from simulacoes where rota_id = {rota_id}', cnx)
#
#     simulated_df['score'] = get_fuzzy_results(simulated_df)
#     agg_simulated_df = simulated_df.groupby(['saida', 'cenario']).agg({'score': 'mean'})


@cronometro
def simulate(cnx, rota_id):
    """
    Função principal responsável por simular as condições climáticas em todos os pontos da rota desejada,
    de forma a encontrar o melhor horário de saída em virtude da preservação da qualidade de sementes

    :param cnx: conexão com o banco de dados local
    :param rota_id: identificador da rota de movimentação de soja (por ex: Barreiras -> Cuiabá)
    :return: melhor horário de saída para a rota no dia simulado
    """
    # Pré-processamento:
    get_clima_por_hora(cnx, rota_id)

    # Processamento:
    simulate_por_hora(cnx, rota_id)
    # get_results(cnx, rota_id)
