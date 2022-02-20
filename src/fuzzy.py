import numpy as np

import skfuzzy as fuzz
from skfuzzy import control as ctrl

# from src.utils import cronometro


# @cronometro
def get_fuzzy_results(simulated_df):
    """
    Implementa sistema de lógica fuzzy (nebulosa) para analisar de forma subjetiva a relação entre
    temperatura e umidade.

    :param simulated_df: conjunto de dados com as combinações de temperatura e umidade simuladas para o índice
                         composto por horário de saída, cidade, mês, dia e hora
    :return: score calculado com base no impacto das condições climáticas sobre a rota (quanto maior, melhor)
    """
    # Consistências de dados:
    # umidade deve ter intervalo entre 0 a 100 (%)
    simulated_df.loc[simulated_df['umidade'] > 100, 'umidade'] = 100
    simulated_df.loc[simulated_df['umidade'] < 0, 'umidade'] = 0

    # # temperatura, para este caso, está com intervalo fixo entre 0 a 40 ºC
    simulated_df.loc[simulated_df['temperatura'] > 40, 'temperatura'] = 40
    simulated_df.loc[simulated_df['temperatura'] < 0, 'temperatura'] = 0

    data_for_temp = (
        # np.arange(0, 40, 1)                            # denso
        simulated_df['temperatura'].sort_values().values   # esparso
    )
    data_for_umid = (
        # np.arange(0, 101, 1)                              # denso
        simulated_df['umidade'].sort_values().values        # esparso
    )

    # Declarando variáveis de entrada
    temperatura = ctrl.Antecedent(universe=data_for_temp, label='Temperatura')
    umidade = ctrl.Antecedent(universe=data_for_umid, label='Umidade')

    # Declarando variáveis de saída
    score = ctrl.Consequent(universe=np.arange(0, 11, 1), label='Score', defuzzify_method='centroid')

    # Declarando funções de pertinência
    # Temperatura:
    temperatura['Baixa'] = fuzz.trapmf(temperatura.universe, [0, 0, 10, 25])
    temperatura['Media'] = fuzz.trimf(temperatura.universe, [20, 25, 30])
    temperatura['Alta'] = fuzz.trapmf(temperatura.universe, [25, 35, 40, 40])

    # Umidade:
    umidade['Baixa'] = fuzz.trapmf(umidade.universe, [0, 0, 30, 50])
    umidade['Media'] = fuzz.trimf(umidade.universe, [40, 60, 80])
    umidade['Alta'] = fuzz.trapmf(umidade.universe, [70, 90, 100, 100])

    # Score:
    score['Bom'] = fuzz.gaussmf(score.universe, mean=10, sigma=2)
    score['Medio'] = fuzz.trimf(score.universe, [4, 5, 6])
    score['Ruim'] = fuzz.gaussmf(score.universe, mean=0, sigma=2)

    # Visualização das funções de pertinência
    # temperatura.view()
    # umidade.view()
    # score.view()

    # Conjunto de regras consideradas pela lógica fuzzy
    rules = {
        # baixas temperaturas:
        temperatura['Baixa'] & umidade['Baixa']: score['Bom'],
        temperatura['Baixa'] & umidade['Media']: score['Bom'],
        temperatura['Baixa'] & umidade['Alta']: score['Ruim'],

        # médias temperaturas:
        temperatura['Media'] & umidade['Baixa']: score['Bom'],
        temperatura['Media'] & umidade['Media']: score['Medio'],
        temperatura['Media'] & umidade['Alta']: score['Ruim'],

        # altas temperaturas:
        temperatura['Alta'] & umidade['Baixa']: score['Medio'],
        temperatura['Alta'] & umidade['Media']: score['Ruim'],
        temperatura['Alta'] & umidade['Alta']: score['Ruim']
    }

    controlador = ctrl.ControlSystem(rules=[ctrl.Rule(key, value) for key, value in rules.items()])
    simulador = ctrl.ControlSystemSimulation(controlador)

    # Chamando a simulação:
    simulador.input['Temperatura'] = simulated_df['temperatura'].values
    simulador.input['Umidade'] = simulated_df['umidade'].values

    simulador.compute()
    # simulated_df['score'] = simulador.output['Score']

    return simulador.output['Score']
