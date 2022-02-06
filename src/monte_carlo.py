import numpy as np
import scipy.stats as st

from src.utils import cronometro, logging


SIZE = 1000


# @cronometro
def normal_transformation(scale, shape):
    # referência: (Banks, 2010, p. 343)

    u1 = np.random.uniform(0, 1, size=int(SIZE / 2))
    u2 = np.random.uniform(0, 1, size=int(SIZE / 2))

    sqrt_of_log = np.sqrt(-2 * np.log(u1))
    calculated_u2 = 2 * np.pi * u2

    x1 = scale + shape * sqrt_of_log * np.cos(calculated_u2)
    x2 = scale + shape * sqrt_of_log * np.sin(calculated_u2)

    return np.concatenate((x1, x2))


# @cronometro
def log_norm_transformation(scale, shape):
    # referência: (Banks, 2010, p. 343)

    return np.log(normal_transformation(scale, shape))


# @cronometro
def weibull_transformation(scale, shape):
    # referência: (Banks, 2010, p. 323)

    u1 = np.random.uniform(0, 1, size=SIZE)
    x1 = scale * np.power(np.log(u1) * (-1), 1 / shape)

    return x1


# @cronometro
def expon_transformation(scale, shape):  # noqa
    # referência: (Banks, 2010, p. 319)

    expon_lambda = 1 / scale

    u1 = np.random.uniform(0, 1, size=SIZE)
    x1 = (-1 / expon_lambda) * np.log(u1)

    return x1


# @cronometro
def gamma_transformation(scale, shape):
    # referência: (Banks, 2010, p. 340)

    a = np.sqrt(1 / ((2 * shape) - 1))
    b = shape - np.log(4)

    x = np.array([])
    needed_size = SIZE

    while x.size <= SIZE:
        r1 = np.random.uniform(0, 1, size=needed_size)
        r2 = np.random.uniform(0, 1, size=needed_size)

        v = r1 / (1 - r1)
        x = shape * np.power(v, scale)

        acceptance = x <= b + (shape * a + 1) * np.log(v) - np.log(np.power(r1, 2) * r2)
        x = x[acceptance]

        needed_size = SIZE - x.size

    return x[:1000]


# @cronometro
def log_gamma_transformation(scale, shape):

    return np.log(gamma_transformation(scale, shape))


# Mapeamento para chamada em 'simulate.py'
DIST_x_FUNC = {'norm':      normal_transformation,
               'lognorm':   log_norm_transformation,
               'weibull':   weibull_transformation,
               'expon':     expon_transformation,
               'gamma':     gamma_transformation,
               'loggamma':  log_gamma_transformation}


# @cronometro
def best_fit_distribution(data):
    y, x = np.histogram(data['value'].values, bins=200, density=True)
    x = (x + np.roll(x, -1))[:-1] / 2.0

    best_distribution = None
    best_params = None
    menor_erro = 100000

    for distribution in DIST_x_FUNC.keys():
        distribution = getattr(st, distribution)

        try:
            params = distribution.fit(x)

            arg = params[:-2]
            loc = params[-2]
            scale = params[-1]

            pdf = distribution.pdf(x, loc=loc, scale=scale, *arg)
            sse = np.sum(np.power(y - pdf, 2.0))

            if sse < menor_erro:
                best_distribution = distribution.name
                best_params = ','.join(map(str, [scale, loc]))
                menor_erro = sse

        except Exception:  # noqa
            logging.info(f"Não foi possível ajustar a distribuição '{distribution.name}'")

    return best_distribution, best_params
