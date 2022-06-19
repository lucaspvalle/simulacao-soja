import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from src.utils import cronometro

configs = {"palette": "Paired", "linewidth": 1.25}


@cronometro
def get_boxplot(simulated_df):
    """
    Constrói gráficos de boxplot para mostrar a dispersão dos dados simulados.

    :param simulated_df: conjunto de dados simulados.
    :return: gráfico de boxplot.
    """

    # medidas a serem apresentadas no gráfico e os limites do eixo y
    plots = {
        'score': (0, 100),
        'temperatura': (-5, 45),
        'umidade': (-5, 105)
    }

    for medida in list(plots.keys()):
        fig, ax = plt.subplots(figsize=(8, 6), constrained_layout=True)
        sns.boxplot(x='hora', y=medida, data=simulated_df, ax=ax, whis=1.5, fliersize=2, **configs)

        plt.xlabel("Horário de saída")
        plt.ylabel(medida.capitalize())

        plt.ylim(plots[medida])
        plt.show()


def get_duracao_de_medidas(simulated_df):
    """
    Constrói um gráfico de barras empilhadas para mostrar o tempo de exposição provável da rota
    sob faixas de temperatura e umidade.

    :param simulated_df: conjunto de dados simulados.
    :return: gráfico de barras empilhadas.
    """

    for medida in ['temperatura', 'umidade']:
        intervalos = pd.cut(simulated_df[medida], bins=5)

        # criando os grupos
        df = simulated_df.groupby(by=['saida', intervalos]).agg({'duracao': 'sum'}).reset_index()
        totais = simulated_df.groupby(by=['saida']).agg({'duracao': 'sum'})

        # calculando a frequência
        df = df.merge(totais, on='saida', suffixes=('', '_total'))
        df['frequencia'] = df['duracao'] / df['duracao_total']
        df = df.drop(columns=['duracao', 'duracao_total'])

        # formatando para stacked bar plot
        df['saida'] = df['saida'].dt.strftime("%H:%M")
        df = df.set_index(['saida', medida])['frequencia']

        fig, ax = plt.subplots(figsize=(8, 6), constrained_layout=True)
        df.unstack().plot(kind='barh', stacked=True, ax=ax)

        plt.title(f"Exposição para {medida}")
        plt.ylabel("Horário de saída")
        plt.xlabel("Exposição")

        ax.legend(title=f"Faixas de {medida}")
        ax.legend(loc='center left', bbox_to_anchor=(1.01, 0.5))

        ax.set_xticklabels(['{:,.2%}'.format(x) for x in ax.get_xticks()])

        plt.show()


def get_linha_do_tempo(simulated_df):
    """
    Constrói um gráfico temporal das medidas de temperatura e umidade por horário de passagem para
    cada possibilidade de saída.

    Observação: não está sendo utilizado, pois a informação visual é muito poluída.

    FIXME:  a identificação de hora precisa de possuir o dia associado para não haver sobreposição de
            horas de dias diferentes.

    :param simulated_df: conjunto de dados simulados.
    :return: linha do tempo.
    """
    df = (simulated_df.groupby(by=['rota_id', 'saida', 'hora'])
          .agg({'temperatura': 'mean', 'umidade': 'mean'})
          .reset_index()
          .sort_values(by='hora'))

    fig, ax = plt.subplots(figsize=(8, 6), constrained_layout=True)
    ax = sns.lineplot(data=df, x='hora', y='temperatura', hue='saida', **configs)
    ax.tick_params(axis='x', labelrotation=45)

    plt.show()
