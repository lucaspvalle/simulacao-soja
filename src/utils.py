import time
import logging


FORMATTER = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%d/%m/%Y %H:%M:%S")
LOG_FILE = "simulation.log"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
fh = logging.FileHandler(LOG_FILE, mode='w', delay=False)

ch.setFormatter(FORMATTER)
fh.setFormatter(FORMATTER)

logger.addHandler(ch)
logger.addHandler(fh)


def cronometro(func):
    """
    Função de suporte para a contagem de tempo gasta em cada parte do sistema.

    :param func: função a ser medida
    :return: empacotamento do cronometro
    """
    def wrapper(*args, **kwargs):
        inicio = time.time()
        func(*args, **kwargs)
        fim = time.time()

        logger.info(f"'{func.__name__}' ({(fim - inicio):.4f} segundos)")

    return wrapper
