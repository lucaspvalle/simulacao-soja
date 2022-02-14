import numpy as np

import skfuzzy as fuzz
from skfuzzy import control as ctrl

from src.utils import cronometro


# @cronometro
def get_fuzzy_results(simulated_df):
    """
    Implementa sistema de lógica fuzzy (nebulosa) para analisar de forma subjetiva a relação entre
    temperatura e umidade.

    TODO: buscar as relações ambientais mais desejadas para a qualidade de sementes

    :param simulated_df: conjunto de dados com as combinações de temperatura e umidade simuladas para o índice
                         composto por horário de saída, cidade, mês, dia e hora
    :return: score calculado com base no impacto das condições climáticas sobre a rota (quanto maior, melhor)
    """
    # Consistências de dados:
    # umidade deve ter intervalo entre 0 a 100 (%)
    simulated_df.loc[simulated_df['umidade'] > 100, 'umidade'] = 100
    simulated_df.loc[simulated_df['umidade'] < 0, 'umidade'] = 0

    # FIXME: analisar esse intervalo
    # temperatura, para este caso, está com intervalo fixo entre -10 a 50 ºC
    simulated_df.loc[simulated_df['temperatura'] > 50, 'temperatura'] = 50
    simulated_df.loc[simulated_df['temperatura'] < -10, 'temperatura'] = -10

    data_for_temp = (
        # np.arange(-10, 51, 1)                            # denso
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
    temperatura['Baixa'] = fuzz.trapmf(temperatura.universe, [-10, -10, 5, 20])
    temperatura['Media'] = fuzz.trimf(temperatura.universe, [15, 25, 30])
    temperatura['Alta'] = fuzz.trapmf(temperatura.universe, [25, 35, 50, 50])

    # Umidade:
    umidade['Baixa'] = fuzz.trapmf(umidade.universe, [0, 0, 10, 20])
    umidade['Media'] = fuzz.trimf(umidade.universe, [15, 30, 50])
    umidade['Alta'] = fuzz.trapmf(umidade.universe, [45, 60, 100, 100])

    # Score:
    score['Bom'] = fuzz.gaussmf(score.universe, mean=10, sigma=3.2)
    score['Ruim'] = fuzz.gaussmf(score.universe, mean=0, sigma=3.2)

    # Visualização das funções de pertinência
    # temperatura.view()
    # umidade.view()
    # score.view()

    # Conjunto de regras consideradas pela lógica fuzzy
    rules = {
        # baixas temperaturas:
        temperatura['Baixa'] & umidade['Baixa']: score['Bom'],
        temperatura['Baixa'] & umidade['Media']: score['Bom'],
        temperatura['Baixa'] & umidade['Alta']: score['Bom'],

        # médias temperaturas:
        temperatura['Media'] & umidade['Baixa']: score['Bom'],
        temperatura['Media'] & umidade['Media']: score['Ruim'],
        temperatura['Media'] & umidade['Alta']: score['Ruim'],

        # altas temperaturas:
        temperatura['Alta'] & umidade['Baixa']: score['Bom'],
        temperatura['Alta'] & umidade['Media']: score['Ruim'],
        temperatura['Alta'] & umidade['Alta']: score['Ruim']
    }

    controlador = ctrl.ControlSystem(rules=[ctrl.Rule(key, value) for key, value in rules.items()])
    simulador = ctrl.ControlSystemSimulation(controlador)

    # Chamando a simulação:
    simulador.input['Temperatura'] = simulated_df['temperatura'].values
    simulador.input['Umidade'] = simulated_df['umidade'].values

    simulador.compute()
    simulated_df['score'] = simulador.output['Score']

    return simulated_df
