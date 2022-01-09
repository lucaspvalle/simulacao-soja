import sqlite3
import logging
from src.integrate import read_historical_data
from src.simulate import simulate_temperatura_por_hora


def main():
    logging.info('Iniciando!')
    cnx = sqlite3.connect('data.sqlite')

    read_historical_data(cnx, atualizar_base=False)
    simulate_temperatura_por_hora(cnx, rota_id=1, respeita_turno=True)

    logging.info('Fim!')


if __name__ == "__main__":
    main()
