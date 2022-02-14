import numpy as np
from distfit import distfit

# from src.utils import cronometro


SIZE = 1000


# @cronometro
def normal_transformation(mu, std):
    # referência: (Banks, 2010, p. 343)

    u1 = np.random.uniform(0, 1, size=int(SIZE / 2))
    u2 = np.random.uniform(0, 1, size=int(SIZE / 2))

    sqrt_of_log = np.sqrt(-2 * np.log(u1))
    calculated_u2 = 2 * np.pi * u2

    x1 = mu + std * sqrt_of_log * np.cos(calculated_u2)
    x2 = mu + std * sqrt_of_log * np.sin(calculated_u2)

    return np.concatenate((x1, x2))


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
        generated_x = shape * np.power(v, a)

        acceptance = generated_x <= b + (shape * a + 1) * np.log(v) - np.log(np.power(r1, 2) * r2)
        x = np.concatenate((x, generated_x[acceptance] / (scale * shape)))

        needed_size = (SIZE - x.size) * 2

    return x[:1000]


# @cronometro
def beta_transformation(a, b, loc, scale):

    y1 = gamma_transformation(a, 1)
    y2 = gamma_transformation(b, 1)

    x1 = y1 / (y1 + y2)

    shifted_x1 = x1 * scale + loc

    return shifted_x1


# @cronometro
def triang_transformation(lower, upper, mode):

    u1 = np.random.uniform(0, 1, size=SIZE)

    x1 = np.where(u1 <= (mode - lower) / (upper - lower),
                  lower + np.sqrt(u1 * (mode - lower) * (upper - lower)),
                  upper - np.sqrt((1 - u1) * (upper - mode) * (upper - lower)))

    return x1


# Mapeamento para chamada em 'simulate.py'
DIST_x_FUNC = {'norm':      normal_transformation,
               # 'expon':     expon_transformation,
               'gamma':     gamma_transformation,
               'beta':      beta_transformation,
               'triang':    triang_transformation}


# @cronometro
def best_fit_distribution(data):
    distributions_to_fit = list(DIST_x_FUNC.keys())

    dist = distfit(distr=distributions_to_fit)
    results = dist.fit_transform(data['value'], verbose=0)['model']

    params = ','.join(map(str, results['params']))
    return results['name'], params
