import numpy as np
from distfit import distfit


SIZE = 1000


def normal_transformation(loc, scale):
    """
    Geração de números aleatórios conforme a Distribuição Normal

    Referência de implementação: Banks, 2010, p. 343

    :param loc:   média da distribuição
    :param scale: variância da distribuição
    :return:      números aleatórios
    """
    return np.random.default_rng().normal(loc=loc, scale=scale, size=SIZE)


def expon_transformation(scale, shape):  # noqa
    """
    Geração de números aleatórios conforme a Distribuição Exponencial

    Referência de implementação: Banks, 2010, p. 319

    :param scale: taxa média entre chegadas
    :param shape: não é utilizado
    :return:      números aleatórios
    """
    return np.random.default_rng().exponential(scale=scale, size=SIZE)


def gamma_transformation(shape, loc, scale):
    """
    Geração de números aleatórios conforme a Distribuição Gama

    Referência de implementação:
        Banks, 2010, p. 340     - shape >= 1
        Law, 2015, p. 455       - shape < 1

    :param shape: beta
    :param loc:   localização da distribuição
    :param scale: alpha
    :return:      números aleatórios
    """
    return np.random.default_rng().gamma(shape=shape, scale=scale, size=SIZE) + loc


def beta_transformation(a, b, loc, scale):  # noqa
    """
    Geração de números aleatórios conforme a Distribuição Beta

    Referência de implementação: Law, 2015, p. 458
    Obs: desativado porque apenas é válido para a, b > 0

    :param a: a
    :param b: b
    :param loc: localização da distribuição
    :param scale: -
    :return: números aleatórios
    """
    pass


def triang_transformation(lower, upper, mode):
    """
    Geração de números aleatórios conforme a Distribuição Triangular

    Referência de implementação:

    :param lower: valor à esquerda (ponta do triângulo)
    :param upper: valor à direita (ponta do triângulo)
    :param mode:  valor central (pico do triângulo)
    :return: números aleatórios
    """
    # consistência: por algum motivo, existem distribuições ajustadas com mode > upper
    if mode > upper:
        mode, upper = upper, mode

    return np.random.default_rng().triangular(left=lower, mode=mode, right=upper, size=SIZE)


def uniform_transformation(loc, scale):
    """
    Geração de números aleatórios conforme a Distribuição Uniforme

    Referência de implementação: Banks, 2010, p. 321

    :param loc:   valor mínimo da distribuição
    :param scale: tamanho do intervalo entre os pontos mínimo e máximo
    :return:      números aleatórios
    """
    return np.random.default_rng().uniform(low=loc, high=loc + scale, size=SIZE)


# Mapeamento para chamada em 'simulate.py'
DIST_x_FUNC = {'norm':      normal_transformation,
               # 'expon':     expon_transformation,
               'gamma':     gamma_transformation,
               # 'beta':      beta_transformation,
               'triang':    triang_transformation,
               'uniform':   uniform_transformation}


def best_fit_distribution(data):
    """
    Função responsável por avaliar a distribuição com melhor ajuste aos conjuntos de dados, considerando
    o método SSE para as distribuições disponíveis em 'DIST_x_FUNC'

    :param data: conjunto de dados avaliado
    :return:     a melhor distribuição ajustada e seus parâmetros
    """

    distributions_to_fit = list(DIST_x_FUNC.keys())

    dist = distfit(distr=distributions_to_fit, bins=int(np.sqrt(len(data))))
    results = dist.fit_transform(data, verbose=0)['model']

    return results['name'], ','.join(map(str, results['params']))
