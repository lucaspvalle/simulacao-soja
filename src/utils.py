import time
import logging


logging.basicConfig(encoding='utf-8',  format="[%(asctime)s] %(message)s", datefmt="%d-%m-%y %H:%M:%S",
                    handlers=[logging.FileHandler("simulation.log", mode='w', delay=False), logging.StreamHandler()],
                    level=logging.INFO)

# FORMATTER = logging.Formatter("[%(asctime)s] %(message)s")
# LOG_FILE = "simulation.log"
#
# console_handler = logging.StreamHandler()
# console_handler.setFormatter(FORMATTER)
#
# file_handler = logging.FileHandler(LOG_FILE, mode='w', delay=False)
# file_handler.setFormatter(FORMATTER)
#
# logger = logging.getLogger(LOG_FILE)
# logger.setLevel(logging.DEBUG)
#
# logger.addHandler(console_handler)
# logger.addHandler(file_handler)


def cronometro(func):
    """
    Função de suporte para a contagem de tempo gasta em cada parte do sistema.

    FIXME: não deveria quebrar o código...

    :param func: função a ser medida
    :return: empacotamento do cronometro
    """
    def wrapper(*args, **kwargs):
        inicio = time.time()
        try:
            func(*args, **kwargs)
        except Exception as Erro:  # noqa
            logging.info("Erro: " + str(Erro))
            pass
        fim = time.time()

        logging.info(f"'{func.__name__}' ({(fim - inicio):.4f} segundos)")

    return wrapper
