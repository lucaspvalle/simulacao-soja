import numpy as np
import scipy.stats as st

from src.utils import cronometro, logging


# @cronometro
def best_fit_distribution(data):
    y, x = np.histogram(data['value'].values, bins=200, density=True)
    x = (x + np.roll(x, -1))[:-1] / 2.0

    best_distribution = [None, None]
    menor_erro = 100000

    for distribution in ['norm', 'dweibull', 'loggamma', 'beta', 'uniform', 'expon', 'triang']:
        distribution = getattr(st, distribution)

        try:
            params = distribution.fit(x)

            arg = params[:-2]
            loc = params[-2]
            scale = params[-1]

            pdf = distribution.pdf(x, loc=loc, scale=scale, *arg)
            sse = np.sum(np.power(y - pdf, 2.0))  # métrica de erro: sum of squares for error

            if sse < menor_erro:
                best_distribution = [distribution.name, str(params)]
                menor_erro = sse

        except Exception:  # noqa
            logging.info(f"Não foi possível ajustar a distribuição '{distribution.name}'")

    return best_distribution


@cronometro
def normal_transformation(data):
    # nome do algoritmo
    u1 = np.random.uniform(0, 1, size=1)
    u2 = np.random.uniform(0, 1, size=1)

    sqrt_of_log = np.sqrt(-2 * np.log(u1))
    calculated_u2 = 2 * np.pi * u2

    x1 = sqrt_of_log * np.cos(calculated_u2)
    x2 = sqrt_of_log * np.sin(calculated_u2)


@cronometro
def weibull_transformation(data):
    pass


@cronometro
def loggamma_transformation(data):
    pass


@cronometro
def beta_transformation(data):
    alpha = 0.95
    beta = 0.98

    # Johnk’s Algorithm
    if alpha < 1 and beta < 1:
        u1 = np.random.uniform(0, 1, size=1)
        u2 = np.random.uniform(0, 1, size=1)

        v1 = np.power(u1, 1 / alpha)
        v2 = np.power(u2, 1 / beta)

        if v1 + v2 <= 1:
            x = v1 / (v1 + v2)


@cronometro
def uniform_transformation(data):
    pass


@cronometro
def expon_transformation(data):
    pass


@cronometro
def triang_transformation(data):
    pass
