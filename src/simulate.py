import logging
import pandas as pd
from distfit import distfit
from datetime import datetime, timedelta
from src.cronometro import cronometro


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

    # horizonte de planejamento:
    primeiro_dia = datetime.strptime('2021-06-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    ultimo_dia = primeiro_dia + timedelta(days=7)  # TODO: parametrizar de acordo com o transit time

    dm_query = (f"select c.cidade_id, c.cidade, dm.* from dados_metereologicos dm "
                f"inner join cidades c using (estacao_id) "
                f"where dm.mes between {primeiro_dia.month} and {ultimo_dia.month} "
                f"  and dm.dia between {primeiro_dia.day} and {ultimo_dia.day}")

    dm = pd.read_sql(dm_query, cnx, parse_dates=['timestamp'])
    itinerario = pd.read_sql(f"select * from transit_time where rota_id = {rota_id}", cnx).assign(inicio=None, fim=None)
    rotas = pd.read_sql(f"select * from rotas where rota_id = {rota_id}", cnx)

    origem_da_rota = rotas['origem'].iloc[0]
    destino_da_rota = rotas['destino'].iloc[0]
    primeiro_dia = dm['timestamp'].dt.strftime('%Y-%m-%d').unique()[0]

    # inicializa possíveis horários de saída em horários comerciais (exclui horário de almoço):
    # TODO: ver com caminhoneiros que horários são esses
    horario_de_trabalho = [hora for hora in range(0, 24, 1)
                           if (8 <= hora <= 18 and hora != 12) or not respeita_turno]
    saidas = [pd.to_datetime(primeiro_dia + " " + str(hora).zfill(2) + ":00") for hora in horario_de_trabalho]

    for horario in saidas:
        logging.info(f'(Rota: {rota_id}) Saida: {horario}')
        horario_inicial = horario

        origem = origem_da_rota
        destino = None

        while destino != destino_da_rota:
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

        dataset = (itinerario
                   .merge(dm, left_on=['origem'], right_on=['cidade_id'])
                   # não podemos filtrar pelo timestamp porque queremos dados de anos passados
                   .query('hora >= inicio.dt.time and hora <= fim.dt.time '
                          'and mes >= inicio.dt.month and mes <= fim.dt.month '
                          'and dia >= inicio.dt.day and dia <= fim.dt.day')
                   )[['cidade_id', 'cidade', 'mes', 'dia', 'hora', 'temperatura', 't_max', 't_min',
                      'umidade', 'u_max', 'u_min']]

        simulate_distribuicoes(cnx, dataset)


@cronometro
def simulate_distribuicoes(cnx, dataset):
    """
    Infere a distribuição de probabilidade que melhor representa as condições climáticas de cada local da rota
    em determinado horário

    :param cnx: conexão com o banco de dados local
    :param dataset: conjunto de dados com a data (dia e hora) em que o veículo estará em cada cidade
    """
    cnx.execute("delete from distribuicoes")
    cnx.commit()

    # FIXME: duplicar conteúdo para umidade

    # o banco de dados do INMET disponibiliza dados atuais, máximos e mínimos
    # utilizamos ambos como ocorrências de temperaturas em determinada hora para aumentar o conjunto de dados
    df_temp = (pd.melt(dataset, id_vars=['cidade_id', 'cidade', 'mes', 'dia', 'hora'],
                       value_vars=['temperatura', 't_max', 't_min'], value_name='value')
               .drop(columns=['variable'])
               .dropna(subset=['value']))

    dist_por_hora = []

    dist = distfit()

    for index, values in df_temp.groupby(['cidade', 'mes', 'dia', 'hora']):
        cidade_id = values['cidade_id'].iloc[0]
        cidade = values['cidade'].iloc[0]
        mes = str(values['mes'].iloc[0]).zfill(2)  # convertendo mês 6 para 06 (mês 10 continua 10)
        dia = str(values['dia'].iloc[0]).zfill(2)  # convertendo dia 9 para 09 (dia 10 continua 10)
        hora = values['hora'].iloc[0]

        logging.info(f"{cidade} ({dia}/{mes} {hora})")

        dist.fit_transform(values['value'])  # avalia a melhor distribuição que se ajusta ao conjunto de dados
        dist_name = dist.model['name']       # retorna o nome da distribuição
        params = dist.model['params']        # retorna a parametrização da distribuição

        dist_por_hora.append((cidade_id, mes, dia, str(hora), dist_name, str(params)))

    (pd.DataFrame(dist_por_hora, columns=['cidade_id', 'mes', 'dia', 'hora', 'dist_name', 'params'])
     .to_sql('distribuicoes', cnx, if_exists='append', index=False))


@cronometro
def simulate(cnx, rota_id, respeita_turno):
    get_clima_por_hora(cnx, rota_id, respeita_turno)
