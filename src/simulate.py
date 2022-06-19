import pandas as pd
from datetime import datetime, timedelta

from src.utils import cronometro, logger

from src.plot import get_boxplot, get_duracao_de_medidas
from src.fuzzy import get_fuzzy_results
from src.monte_carlo import DIST_x_FUNC, best_fit_distribution


class Simulador:
    def __init__(self, cnx):
        """
        Função principal responsável por simular as condições climáticas em todos os pontos da rota desejada
        para encontrar o melhor horário de saída em virtude da preservação da qualidade de sementes

        :param cnx: conexão com o banco de dados local
        :return: melhor horário de saída para a rota no dia simulado
        """
        self.cnx = cnx
        rotas = self.cnx.execute("select rota_id, origem.cidade, destino.cidade from rotas " +
                                 "inner join cidades origem on (origem.cidade_id = rotas.origem) " +
                                 "inner join cidades destino on (destino.cidade_id = rotas.destino) " +
                                 "where rotas.ativo = 1").fetchall()

        for rota_id, origem, destino in rotas:
            logger.info(f"Rota em análise: {origem} -> {destino} (ID: {rota_id})")
            self.rota_id = rota_id

            # Pré-processamento:
            self.get_clima_por_hora()

            # Processamento:
            self.simulate_por_hora()

            # Resultados:
            self.get_results()

    @cronometro
    def get_clima_por_hora(self):
        """
        Analisa o itinerário de cada rota para inferir o horário em que o veículo passará em cada cidade
        Com isso, podemos cruzar com os dados meteorológicos para saber as condições climáticas históricas no local
        """
        logger.info('Analisando itinerário da rota!')

        origem_rota, destino_rota, primeiro_dia = \
            self.cnx.execute(f"select origem, destino, inicio from rotas where rota_id = {self.rota_id}").fetchone()
        itinerario = (pd.read_sql(f"select * from transit_time where rota_id = {self.rota_id}", self.cnx)
                      .assign(inicio=None, fim=None))

        # horizonte de planejamento:
        primeiro_dia = datetime.strptime(primeiro_dia + " 00:00:00", '%Y-%m-%d %H:%M:%S')
        ultimo_dia = primeiro_dia + timedelta(minutes=int(itinerario['transit_time'].sum()), days=1)

        dm_query = (f"select c.cidade_id, c.cidade, dm.* from dados_metereologicos dm "
                    f"inner join cidades c using (estacao_id) "
                    f"where cast(strftime("'"%m"'f", dm.timestamp) as integer) " +
                    f"      between {primeiro_dia.month} and {ultimo_dia.month} " +
                    f"  and cast(strftime("'"%d"'f", dm.timestamp) as integer) " +
                    f"      between {primeiro_dia.day} and {ultimo_dia.day}")

        dm = pd.read_sql(dm_query, self.cnx, parse_dates=['timestamp'])
        dm['mes'] = dm['timestamp'].dt.month
        dm['dia'] = dm['timestamp'].dt.day
        dm['hora'] = dm['timestamp'].dt.time

        # inicializa possíveis horários de saída em horários comerciais:
        saidas = [primeiro_dia + timedelta(hours=hora) for hora in range(6, 19, 1)]

        for horario in saidas:
            logger.info(f'(Rota: {self.rota_id}) Saida: {horario}')
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

                # caminhoneiros são obrigados legalmente a parar por 8 horas ininterruptas após 24h de viagem
                if dias_de_viagem >= (24 * 60):
                    horario_final += timedelta(hours=8)
                    horas_de_viagem = 0
                    dias_de_viagem = 0

                itinerario.loc[itinerario['origem'] == origem, 'inicio'] = horario_inicial
                itinerario.loc[itinerario['origem'] == origem, 'fim'] = horario_final

                horario_inicial = horario_final
                origem = destino
            # endwhile

            itinerario['inicio'] = pd.to_datetime(itinerario['inicio'])
            itinerario['fim'] = pd.to_datetime(itinerario['fim'])

            itinerario['duracao'] = ((itinerario['fim'] - itinerario['inicio']).dt.seconds / 60).astype(int)

            # primeiro_horario_de_viagem = itinerario['inicio'].min()

            hora_maxima = int(divmod((itinerario['fim'].max() - horario).total_seconds(), 3600)[0])
            guia_de_horarios = (pd.DataFrame(columns=['inicio', 'fim'])
                                # somamos 2 para finalizar com o horário "acima" do último
                                # por exemplo: se o trajeto termina às 14h, queremos que o guia vá até às 15h
                                # para que quando o merge seja feito com dm, os dados de 14h sejam inclusos
                                # porque hora do dado meteorológico <= hora de passagem
                                # lembrando que range usa intervalos [,), ou seja: descarta o último item
                                .assign(fim=[horario + timedelta(hours=x) for x in range(1, hora_maxima + 2)]))

            guia_de_horarios['inicio'] = guia_de_horarios.apply(lambda x: x.fim - timedelta(hours=1), axis=1)
            guia_de_horarios = (guia_de_horarios
                                .merge(itinerario, how='cross', suffixes=('', '_itinerario'))
                                .query('inicio >= inicio_itinerario & fim <= fim_itinerario'))

            # puxa todos os dados históricos de horários no intervalo em que o veículo passa pela localidade
            data = (guia_de_horarios
                    .merge(dm, left_on=['origem'], right_on=['cidade_id'])
                    # não podemos filtrar pelo timestamp porque queremos dados de anos passados
                    .query('hora >= inicio.dt.time and hora <= fim.dt.time '
                           'and mes >= inicio.dt.month and mes <= fim.dt.month '
                           'and dia >= inicio.dt.day and dia <= fim.dt.day')
                    .assign(saida=horario)
                    )[['cidade_id', 'saida', 'hora', 'duracao', 'temperatura', 't_max', 't_min', 'umidade', 'u_max',
                       'u_min']]

            self.get_distribuicoes(data)
        # endfor

    @cronometro
    def get_distribuicoes(self, data):
        """
        Infere a distribuição de probabilidade que melhor representa as condições climáticas de cada local da rota
        em determinado horário

        :param data: conjunto de dados com a data (dia e hora) em que o veículo estará em cada cidade
        """
        saida = str(data['saida'].iloc[0])

        self.cnx.execute(f"delete from distribuicoes where rota_id = {self.rota_id} and saida = '{saida}'")
        self.cnx.commit()

        for medida in ['temperatura', 'umidade']:
            if medida == "temperatura":
                cols = ['temperatura', 't_max', 't_min']
            else:
                cols = ['umidade', 'u_max', 'u_min']

            # o banco de dados do INMET disponibiliza dados atuais, máximos e mínimos
            # utilizamos ambos como ocorrências de temperaturas em determinada hora para aumentar o conjunto de dados
            df = (pd.melt(data, id_vars=['saida', 'cidade_id', 'hora', 'duracao'], value_vars=cols,
                          value_name='value')
                  .drop(columns=['variable'])
                  .dropna(subset=['value']))

            dist_por_hora = []

            for index, values in df.groupby(['saida', 'cidade_id', 'hora']):
                saida, cidade_id, hora = index
                duracao = values['duracao'].iloc[0]

                logger.info(f"Avaliando {medida} para Cidade ID: {cidade_id} (Saída: {saida} -> Hora: {hora})")

                dist_name, params = best_fit_distribution(values['value'])
                dist_por_hora += [(self.rota_id, str(saida), cidade_id, str(hora), duracao, medida, dist_name, params)]

            (pd.DataFrame(dist_por_hora,
                          columns=['rota_id', 'saida', 'cidade_id', 'hora', 'duracao', 'medida', 'dist_name', 'params'])
             .to_sql('distribuicoes', self.cnx, if_exists='append', index=False))

    @cronometro
    def simulate_por_hora(self):
        """
        A partir das distribuições de probabilidade de condições climáticas, calculadas anteriormente
        para os pontos esperados em que o veículo esteja durante a rota, podemos simular diversos cenários
        por meio da geração de números aleatórios que repliquem a realidade.
        """
        distribuicoes = pd.read_sql(f'select * from distribuicoes where rota_id = {self.rota_id}', self.cnx)
        distribuicoes['params'] = distribuicoes['params'].apply(lambda row: [float(item) for item in row.split(',')])

        self.cnx.execute(f'delete from resultados where rota_id = {self.rota_id}')
        self.cnx.execute(f'delete from simulacoes where rota_id = {self.rota_id}')
        self.cnx.commit()

        for index, values in distribuicoes.groupby(['saida', 'cidade_id', 'hora', 'duracao']):
            saida, cidade_id, hora, duracao = index
            hora = str(hora)

            logger.info(f"Ajustando distribuição para Cidade ID: {cidade_id} ({saida} - {hora})")

            simulated_df = pd.DataFrame()
            for medida in ['temperatura', 'umidade']:
                dist_name = values.loc[values['medida'] == medida, 'dist_name'].iloc[0]
                params = values.loc[values['medida'] == medida, 'params'].iloc[0]

                simulated_df[medida] = \
                    DIST_x_FUNC.get(
                        dist_name,
                        lambda *_: logger.info(f"Falha no método de transformação de {dist_name}")  # trata erro
                    )(*params)                                                                      # chama a função

            simulated_df['saida'] = saida

            # exportando resultados agregados apenas em nível de rota, horário de saída e cenário (1..1000)
            (simulated_df.assign(rota_id=self.rota_id, cidade_id=cidade_id, hora=hora, duracao=duracao,
                                 cenario=simulated_df.index + 1)  # noqa
            [['rota_id', 'saida', 'hora', 'cidade_id', 'duracao', 'cenario', 'temperatura', 'umidade']]
            .to_sql('simulacoes', self.cnx, if_exists='append', index=False))

    @cronometro
    def get_results(self):
        logger.info('Calculando resultados!')
        simulated_df = pd.read_sql(f'select * from simulacoes where rota_id = {self.rota_id}',
                                   self.cnx, parse_dates=['saida'])

        simulated_df['dia'] = simulated_df['saida'].dt.strftime("%d/%m/%Y")
        simulated_df['hora'] = simulated_df['saida'].dt.strftime("%H:%M")

        # Consistências de dados:
        # umidade deve ter intervalo entre 0 a 100 (%)
        simulated_df.loc[simulated_df['umidade'] > 100, 'umidade'] = 100
        simulated_df.loc[simulated_df['umidade'] < 0, 'umidade'] = 0

        # temperatura, para este caso, está com intervalo fixo entre 0 a 40 ºC
        simulated_df.loc[simulated_df['temperatura'] > 40, 'temperatura'] = 40
        simulated_df.loc[simulated_df['temperatura'] < 0, 'temperatura'] = 0

        simulated_df['score'] = get_fuzzy_results(simulated_df[['temperatura', 'umidade']])

        get_boxplot(simulated_df)  # analisa a dispersão de resultados entre cenários para um horário de saída
        get_duracao_de_medidas(simulated_df)
