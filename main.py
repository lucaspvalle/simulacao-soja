import sqlite3

from src.simulate import simulate
from src.utils import cronometro, logging
from src.integrate import read_historical_data


@cronometro
def main():
    logging.info('Iniciando!')
    cnx = sqlite3.connect('data.sqlite')

    read_historical_data(cnx, atualizar_base=False)
    simulate(cnx, rota_id=1, respeita_turno=True)

    logging.info('Fim!')


if __name__ == "__main__":
    #try:
        main()
    # except Exception:  # noqa
      #  logging.info(f"Erro: {Exception}!")
