import numpy as np
from distfit import distfit


SIZE = 1000


def normal_transformation(loc, scale):
    return np.random.default_rng().normal(loc=loc, scale=scale, size=SIZE)

    # referência: (Banks, 2010, p. 343)

    # u1 = np.random.uniform(0, 1, size=int(SIZE + 1 / 2))
    # u2 = np.random.uniform(0, 1, size=int(SIZE + 1 / 2))
    #
    # sqrt_of_log = np.sqrt(-2 * np.log(u1))
    # calculated_u2 = 2 * np.pi * u2
    #
    # x1 = loc + scale * sqrt_of_log * np.cos(calculated_u2)
    # x2 = loc + scale * sqrt_of_log * np.sin(calculated_u2)
    #
    # return np.concatenate((x1, x2))[:SIZE]


def expon_transformation(scale, shape):  # noqa
    return np.random.default_rng().exponential(scale=scale, size=SIZE)

    # referência: (Banks, 2010, p. 319)

    # expon_lambda = 1 / scale
    #
    # u1 = np.random.uniform(0, 1, size=SIZE)
    # x1 = (-1 / expon_lambda) * np.log(u1)
    #
    # return x1


def gamma_transformation(shape, loc, scale):
    return np.random.default_rng().gamma(shape=shape, scale=scale, size=SIZE) + loc

    # # referência: (Banks, 2010, p. 340) p/ shape >= 1
    # # referência: (Law, 2015, p. 455) p/ shape < 1
    #
    # needed_size = SIZE
    # x = np.array([])
    #
    # if shape >= 1:
    #     a = np.sqrt(1 / ((2 * shape) - 1))
    #     b = shape - np.log(4)
    #
    #     while x.size < SIZE:
    #         r1 = np.random.uniform(0, 1, size=needed_size)
    #         r2 = np.random.uniform(0, 1, size=needed_size)
    #
    #         v = r1 / (1 - r1)
    #         generated_x = shape * np.power(v, a)
    #
    #         acceptance = generated_x <= b + (shape * a + 1) * np.log(v) - np.log(np.power(r1, 2) * r2)
    #         x = np.concatenate((x, generated_x[acceptance] / (scale * shape)))
    #
    #         needed_size = (SIZE - x.size) * 2
    #
    # else:
    #     b = (np.exp(1) + shape) / np.exp(1)
    #
    #     while x.size < SIZE:
    #         r1 = np.random.uniform(0, 1, size=needed_size)
    #         r2 = np.random.uniform(0, 1, size=needed_size)
    #
    #         P = b * r1
    #
    #         Y_less_than_1 = np.power(P[P <= 1], 1 / shape)
    #         Y_greater_than_1 = - np.log((b - P[P > 1]) / shape)
    #
    #         filtered_Y_less_than_1 = r2[:Y_less_than_1.size] <= np.power(np.exp(1), -1 * Y_less_than_1)
    #         r2 = r2[Y_less_than_1.size:SIZE]  # é necessário ajustar o domínio de r2 para não usar os mesmos valores
    #
    #         filtered_Y_greater_than_1 = r2 <= np.power(Y_greater_than_1, shape - 1)
    #
    #         x = np.concatenate((x, Y_less_than_1[filtered_Y_less_than_1],
    #                             Y_greater_than_1[filtered_Y_greater_than_1]))
    #
    #         x *= scale
    #
    #         needed_size = (SIZE - x.size) * 2
    #
    # x += loc
    # return x[:SIZE]


# def beta_transformation(a, b, loc, scale):
#     # apenas para a > 0 e b > 0
#     return np.random.default_rng().beta(a=a, b=b, size=SIZE) * scale + loc
#
#     y1 = gamma_transformation(a, loc=0, scale=1)
#     y2 = gamma_transformation(b, loc=0, scale=1)
#
#     x1 = y1 / (y1 + y2)
#
#     shifted_x1 = x1 * scale + loc
#
#     return shifted_x1


def triang_transformation(lower, upper, mode):
    if mode > upper:
        mode, upper = upper, mode

    return np.random.default_rng().triangular(left=lower, mode=mode, right=upper, size=SIZE)

    # u1 = np.random.uniform(0, 1, size=SIZE)
    #
    # x1 = np.where(u1 <= (mode - lower) / (upper - lower),
    #               lower + np.sqrt(u1 * (mode - lower) * (upper - lower)),
    #               upper - np.sqrt((1 - u1) * (upper - mode) * (upper - lower)))
    #
    # return x1


def uniform_transformation(loc, scale):
    return np.random.default_rng().uniform(low=loc, high=loc + scale, size=SIZE)

    # u1 = np.random.uniform(0, 1, size=SIZE)

    # a = loc
    # b = loc + scale
    # x1 = a + (b - a) * u1

    # return loc + scale * u1


# Mapeamento para chamada em 'simulate.py'
DIST_x_FUNC = {'norm':      normal_transformation,
               'expon':     expon_transformation,
               'gamma':     gamma_transformation,
               # 'beta':      beta_transformation,
               'triang':    triang_transformation,
               'uniform':   uniform_transformation}


def best_fit_distribution(data):
    distributions_to_fit = list(DIST_x_FUNC.keys())

    dist = distfit(distr=distributions_to_fit, bins=int(np.sqrt(len(data))))
    results = dist.fit_transform(data, verbose=0)['model']

    return results['name'], ','.join(map(str, results['params']))
