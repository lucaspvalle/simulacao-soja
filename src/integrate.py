import os
import pandas as pd
import logging
from src.cronometro import cronometro


data_path = 'data/'


@cronometro
def read_estacoes_inmet(cnx):
    """
    Mapeia estações com informações existentes nos dados disponibilizados pelo INMET e armazena em um banco local

    :param cnx: conexão com o banco de dados local
    """

    cnx.execute('delete from estacoes')
    cnx.commit()

    for arquivo in os.listdir(data_path):
        # logging.info(f'Lendo: {arquivo}')
        regiao, estado, estacao_id, localidade = arquivo.split('_')[1:5]

        caminho = os.path.join(data_path, arquivo)

        # abre o arquivo para armazenar dados geográficos
        # útil caso seja plotado algum mapa, mas caso não seja utilizado, apagar
        df = pd.read_csv(caminho, delimiter=';', nrows=7).T
        latitude = float(df[3].iloc[1].replace(',', '.'))
        longitude = float(df[4].iloc[1].replace(',', '.'))

        cnx.execute(
            f"insert into estacoes values ('{estacao_id}', '{localidade}', '{regiao}', '{estado}', {latitude}, " +
            f"{longitude})")
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
        logging.info('Atualizando base de estações meterológicas!')

        cnx.execute('delete from dados_metereologicos')
        cnx.commit()

        read_estacoes_inmet(cnx)

    # mapeamento de estações cadastradas:
    query = ("select distinct e.estacao_id, e.localidade, e.regiao, e.estado from estacoes e " +
             "inner join cidades c using (estacao_id) " +
             "left join dados_metereologicos dm using (estacao_id) " +
             "where dm.estacao_id is null")

    estacoes_procuradas = cnx.execute(query).fetchall()

    # se não houver necessidade de mapeamento, quebra importação
    if len(estacoes_procuradas) == 0:
        logging.info('Finalizando leitura. Não há necessidade de atualização de estações!')
        return

    # mapeamento de ordem de colunas desejadas:
    mapping = {0: 'data', 1: 'hora', 7: 'temperatura', 9: 't_max', 10: 't_min', 13: 'u_max', 14: 'u_min', 15: 'umidade'}

    # horizonte de planejamento:
    start_date = '2021-06-01 00:00:00'
    end_date = '2021-08-31 23:00:00'

    # leitura de arquivos do INMET:
    for arquivo in estacoes_procuradas:
        estacao_id, localidade, regiao, estado = arquivo

        logging.info(f'Processando: ({estacao_id}) {localidade}-{estado}')

        # Os arquivos seguem a nomenclatura: INMET_NE_BA_A402_BARREIRAS_01-01-2021_A_30-11-2021.CSV
        # INMET_região_estado_[código da estação]_[cidade da estação]_[período inicial]_A_[período final].CSV
        filename = f'INMET_{regiao}_{estado}_{estacao_id}_{localidade}_01-01-2021_A_30-11-2021.CSV'
        caminho = os.path.join(data_path, filename)

        if not os.path.exists(caminho):
            continue

        df = (pd.read_csv(caminho, delimiter=';', header=8, usecols=mapping.keys(), names=mapping.values())
              .assign(estacao_id=estacao_id, timestamp=lambda row: row['data'] + " " + row['hora'].str[:2] + ":00"))

        temp_cols = ['temperatura', 't_max', 't_min']
        df[temp_cols] = df[temp_cols].apply(lambda row: (row.str.replace(',', '.', regex=False).astype('float64')))

        # TODO: tratar missing values de temperatura e umidade...

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.query(f"timestamp >= '{start_date}' and timestamp <= '{end_date}'").drop(columns=['data', 'hora'])

        df.to_sql('dados_metereologicos', cnx, if_exists='append', index=False)
