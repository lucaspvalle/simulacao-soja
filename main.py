import sqlite3

from src.utils import cronometro, logger
from src.simulate import Simulador
from src.integrate import Integrador


@cronometro
def main():
    logger.info('Iniciando!')
    cnx = sqlite3.connect('data.sqlite')

    Integrador(cnx, atualizar_base=False)
    Simulador(cnx)

    logger.info('Fim!')


if __name__ == "__main__":
    try:
        main()
    except Exception as Erro:  # noqa
        logger.info("Erro:" + str(Erro))
