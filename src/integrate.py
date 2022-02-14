import os
import pandas as pd

from src.utils import cronometro, logging


data_path = 'data'


@cronometro
def read_estacoes_inmet(cnx):
    """
    Mapeia estações com informações existentes nos dados disponibilizados pelo INMET e armazena em um banco local

    :param cnx: conexão com o banco de dados local
    """
    cnx.execute('delete from estacoes')
    cnx.commit()

    for ano in os.listdir(data_path):
        logging.info(f'Ano de leitura: {ano}')
        caminho_por_ano = os.path.join(data_path, ano)

        for arquivo in os.listdir(caminho_por_ano):
            # logging.info(f'Arquivo: {arquivo}')
            regiao, estado, estacao_id, localidade = arquivo.split('_')[1:5]

            # os arquivos possuem informação de latitude e longitude das estações
            # útil para plotar gráficos de mapa

            cnx.execute(f"insert or replace into estacoes values ('{estacao_id}', '{localidade}', '{regiao}', "
                        f"'{estado}')")
            cnx.execute(f"insert into dados_estacoes values ('{estacao_id}', {ano}, '{arquivo}')")

            cnx.commit()


@cronometro
def read_historical_data(cnx, atualizar_base=False):
    """
    Armazena os dados metereológicos de estações próximas de cidades que são parte da rota de transporte

    Os dados são obtidos a partir do pacote anual de estações automáticas do INMET
    Estes arquivos devem ser adicionados em uma pasta "data/" na raiz do repositório

    :param cnx: conexão com o banco de dados local
    :param atualizar_base: decide se a tabela será limpa antes do procedimento
    """
    if atualizar_base:
        logging.info('Atualizando base de estações metereológicas!')

        cnx.execute('delete from dados_metereologicos')
        cnx.commit()

        read_estacoes_inmet(cnx)

    # mapeamento de estações cadastradas:
    query = ("select distinct de.estacao_id, c.cidade, c.estado, de.ano, de.arquivo from dados_estacoes de " +
             "inner join cidades c using (estacao_id) " +
             "left join dados_metereologicos dm using (estacao_id, ano) " +
             "where dm.estacao_id is null " +
             "order by de.ano")

    estacoes_procuradas = cnx.execute(query).fetchall()

    # se não houver necessidade de mapeamento, quebra importação
    if len(estacoes_procuradas) == 0:
        logging.info('Finalizando leitura. Não há necessidade de atualização de estações!')
        return

    # mapeamento de ordem de colunas desejadas:
    mapping = {0: 'data', 1: 'hora', 7: 'temperatura', 9: 't_max', 10: 't_min', 13: 'u_max', 14: 'u_min', 15: 'umidade'}

    # leitura de arquivos do INMET:
    for arquivo in estacoes_procuradas:
        estacao_id, localidade, estado, ano, filename = arquivo

        logging.info(f'Processando: ({estacao_id}) {localidade}-{estado} ({ano})')

        caminho = os.path.join(data_path, str(ano), filename)
        if not os.path.exists(caminho):
            continue

        df = (pd.read_csv(caminho, delimiter=';', header=8, usecols=mapping.keys(), names=mapping.values(),
                          encoding='windows-1252', na_values=[-9999])
              .assign(estacao_id=estacao_id, timestamp=lambda row: row['data'] + " " + row['hora'].str[:2] + ":00"))

        for coluna in ['temperatura', 't_max', 't_min']:
            df[coluna] = pd.to_numeric(df[coluna].fillna("").str.replace(',', '.', regex=False), errors='coerce')

        # TODO: tratar missing values de temperatura e umidade...

        # converte UTC para GMT-3
        df['timestamp'] = pd.to_datetime(df['timestamp']) + pd.offsets.Hour(-3)
        df['ano'] = df['timestamp'].dt.year
        df['mes'] = df['timestamp'].dt.month
        df['dia'] = df['timestamp'].dt.day

        df.drop(columns=['data', 'hora'], inplace=True)
        df.dropna(subset=['temperatura', 't_max', 't_min', 'umidade', 'u_max', 'u_min'], how='all', inplace=True)

        df.to_sql('dados_metereologicos', cnx, if_exists='append', index=False)
