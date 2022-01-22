import sqlite3
import logging
from src.integrate import read_historical_data
from src.simulate import simulate


def main():
    logging.info('Iniciando!')
    cnx = sqlite3.connect('data.sqlite')

    read_historical_data(cnx, atualizar_base=False)
    simulate(cnx, rota_id=1, respeita_turno=True)

    logging.info('Fim!')


if __name__ == "__main__":
    main()
