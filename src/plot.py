import matplotlib.pyplot as plt
import seaborn as sns

from src.utils import cronometro


@cronometro
def get_boxplot(simulated_df):
    # medidas a serem apresentadas no gráfico e os limites do eixo y
    plots = {
        'score': (0, 10),
        'temperatura': (-5, 45),
        'umidade': (-5, 105)
    }

    for medida in list(plots.keys()):
        fig, ax = plt.subplots(figsize=(8, 6), constrained_layout=True)
        sns.boxplot(x='hora', y=medida, data=simulated_df, palette="Paired", linewidth=1.25, ax=ax)

        plt.xlabel("Horário de saída")
        plt.ylabel(medida.capitalize())

        plt.ylim(plots[medida])
        plt.show()
