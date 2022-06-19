import os
import pandas as pd

from src.utils import cronometro, logger


class Integrador:
    def __init__(self, cnx, atualizar_base=False):
        """
        Os dados são obtidos a partir do pacote anual de estações automáticas do INMET
        Estes arquivos devem ser adicionados em uma pasta "data/" na raiz do repositório

        Apenas as estações que estão nas rotas cadastradas são mapeadas. As restantes são ignoradas.

        :param cnx: conexão com o banco de dados local
        :param atualizar_base: decide se a tabela será limpa antes do procedimento
        """
        self.data_path = 'data'
        self.cnx = cnx

        with open('db/tables.sql') as file:
            self.create_db_entities(file.read())

        with open('db/views.sql') as file:
            self.create_db_entities(file.read())

        if atualizar_base:
            logger.info('Atualizando base de estações metereológicas!')

            cnx.execute('delete from dados_metereologicos')
            cnx.commit()

            self.read_estacoes_inmet()

            self.read_historical_data()

    def create_db_entities(self, script):
        for query in script.split(';'):
            self.cnx.execute(query)
            self.cnx.commit()

    @cronometro
    def read_estacoes_inmet(self):
        """
        Mapeia estações com informações existentes nos dados disponibilizados pelo INMET e armazena em um banco local
        """
        self.cnx.execute("delete from estacoes")
        self.cnx.execute("delete from dados_estacoes")
        self.cnx.commit()

        df = pd.DataFrame(columns=['estacao_id', 'localidade', 'regiao', 'estado', 'ano', 'arquivo'])
        idx = 0

        for ano in os.listdir(self.data_path):
            logger.info(f'Ano de leitura: {ano}')
            caminho_por_ano = os.path.join(self.data_path, ano)

            for arquivo in os.listdir(caminho_por_ano):
                # logging.info(f'Arquivo: {arquivo}')
                regiao, estado, estacao_id, localidade = arquivo.split('_')[1:5]

                df.loc[idx] = [estacao_id, localidade, regiao, estado, ano, arquivo]
                idx += 1

        (df[['estacao_id', 'localidade', 'regiao', 'estado']]
         .drop_duplicates(subset=['estacao_id'])
         .to_sql('estacoes', self.cnx, if_exists='append', index=False))

        (df[['estacao_id', 'ano', 'arquivo']]
         .drop_duplicates(subset=['estacao_id', 'ano'])
         .to_sql('dados_estacoes', self.cnx, if_exists='append', index=False))

    @cronometro
    def read_historical_data(self):
        """
        Armazena os dados metereológicos APENAS de estações próximas de cidades que são parte da rota de transporte
        """

        # mapeamento de estações cadastradas:
        # a query retorna apenas as estações cadastradas para cidades em percurso de rota e sem dados climáticos
        # populados em banco
        query = ("select distinct de.estacao_id, e.localidade, de.ano, de.arquivo from dados_estacoes de " +
                 "inner join cidades c using (estacao_id) " +
                 "inner join estacoes e using (estacao_id) " +
                 "left join dados_metereologicos dm on " +
                 "  (de.estacao_id = dm.estacao_id and de.ano = strftime('%Y', dm.timestamp)) " +
                 "where dm.estacao_id is null " +
                 "order by de.ano")

        estacoes_procuradas = self.cnx.execute(query).fetchall()

        # se não houver necessidade de mapeamento, quebra importação
        if len(estacoes_procuradas) == 0:
            logger.info('Finalizando leitura. Não há necessidade de atualização de estações!')
            return

        # mapeamento de ordem de colunas desejadas:
        mapping = {0: 'data', 1: 'hora', 7: 'temperatura', 9: 't_max', 10: 't_min', 13: 'u_max', 14: 'u_min',
                   15: 'umidade'}

        # leitura de arquivos do INMET:
        for arquivo in estacoes_procuradas:
            estacao_id, localidade, ano, filename = arquivo

            logger.info(f'Processando: ({estacao_id}) {localidade} ({ano})')

            caminho = os.path.join(self.data_path, str(ano), filename)
            if not os.path.exists(caminho):
                logger.info(f'Arquivo inexistente no diretório do projeto! Pulando...')
                continue

            df = (pd.read_csv(caminho, delimiter=';', header=8, usecols=mapping.keys(), names=mapping.values(),
                              encoding='windows-1252', na_values=[-9999])
                  .assign(estacao_id=estacao_id, timestamp=lambda row: row['data'] + " " + row['hora'].str[:2] + ":00"))

            for coluna in ['temperatura', 't_max', 't_min']:
                df[coluna] = pd.to_numeric(df[coluna].fillna("").str.replace(',', '.', regex=False), errors='coerce')

            # TODO: tratar valores NA de temperatura e umidade...

            # converte UTC para GMT-3
            df['timestamp'] = pd.to_datetime(df['timestamp']) + pd.offsets.Hour(-3)

            df.drop(columns=['data', 'hora'], inplace=True)
            df.dropna(subset=['temperatura', 't_max', 't_min', 'umidade', 'u_max', 'u_min'], how='all', inplace=True)

            df.to_sql('dados_metereologicos', self.cnx, if_exists='append', index=False)
