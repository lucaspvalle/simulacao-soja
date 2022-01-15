import logging
import pandas as pd
from datetime import timedelta
from src.cronometro import cronometro


@cronometro
def simulate_temperatura_por_hora(cnx, rota_id, respeita_turno=True):
    logging.info('Analisando itinerario de cada rota!')

    dm_query = "select c.cidade_id, dm.* from dados_metereologicos dm inner join cidades c using (estacao_id)"
    transit_query = f"select origem, destino, transit_time from transit_time where rota_id = {rota_id}"
    rotas_query = f"select origem, destino from rotas where rota_id = {rota_id}"

    dm = pd.read_sql(dm_query, cnx, parse_dates=['timestamp'])
    itinerario = pd.read_sql(transit_query, cnx).assign(inicio=None, fim=None)
    rotas = pd.read_sql(rotas_query, cnx)

    origem_da_rota = rotas['origem'].iloc[0]
    destino_da_rota = rotas['destino'].iloc[0]
    primeiro_dia = dm['timestamp'].dt.strftime('%Y-%m-%d').unique()[0]

    # inicializa possíveis horários de saída em horários comerciais (exclui horário de almoço):
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

        dataset = (itinerario
                   .merge(dm, left_on=['origem'], right_on=['cidade_id'])
                   .query('timestamp >= inicio and timestamp <= fim')
                   )[['cidade_id', 'timestamp', 'temperatura', 'umidade']]
