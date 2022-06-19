/*Tabelas Cadastrais*/

CREATE TABLE IF NOT EXISTS cidades (
    cidade_id          INTEGER      PRIMARY KEY AUTOINCREMENT
                                    UNIQUE
                                    NOT NULL,
    cidade             VARCHAR (50) NOT NULL,
    estado             VARCHAR (2),
    estacao_id         VARCHAR (10) NOT NULL
                                    REFERENCES estacoes (estacao_id),
    estacao_aproximada BOOLEAN,
    latitude           NUMERIC,
    longitude          NUMERIC
);

CREATE TABLE IF NOT EXISTS estacoes (
    estacao_id VARCHAR (10) NOT NULL
                            PRIMARY KEY,
    localidade VARCHAR (50),
    regiao     VARCHAR (4),
    estado     VARCHAR (4)
);

CREATE TABLE IF NOT EXISTS rotas (
    rota_id INTEGER PRIMARY KEY AUTOINCREMENT
                    NOT NULL
                    UNIQUE,
    origem  INTEGER NOT NULL
                    REFERENCES cidades (cidade_id),
    destino INTEGER NOT NULL
                    REFERENCES cidades (cidade_id),
    inicio  DATE,
    ativo   BOOLEAN
);

CREATE TABLE IF NOT EXISTS transit_time (
    rota_id      INTEGER REFERENCES rotas (rota_id)
                         NOT NULL,
    origem       INTEGER REFERENCES cidades (cidade_id)
                         NOT NULL,
    destino      INTEGER REFERENCES cidades (cidade_id)
                         NOT NULL,
    transit_time INTEGER,
    PRIMARY KEY (
        rota_id,
        origem,
        destino
    )
);

/*Tabelas Calculadas*/

CREATE TABLE IF NOT EXISTS dados_estacoes (
    estacao_id VARCHAR (10)  NOT NULL
                             REFERENCES estacoes (estacao_id),
    ano        INTEGER       NOT NULL,
    arquivo    VARCHAR (100),
    PRIMARY KEY (
        estacao_id,
        ano
    )
);

CREATE TABLE IF NOT EXISTS dados_metereologicos (
    estacao_id  VARCHAR (10) NOT NULL,
    timestamp   DATETIME     NOT NULL,
    temperatura NUMERIC,
    t_max       NUMERIC,
    t_min       NUMERIC,
    umidade     NUMERIC,
    u_max       NUMERIC,
    u_min       NUMERIC,
    PRIMARY KEY (
        estacao_id,
        timestamp
    )
);

/*Tabelas de Saída*/

CREATE TABLE IF NOT EXISTS distribuicoes (
    rota_id   INTEGER      REFERENCES rotas (rotas_id)
                           NOT NULL,
    saida     DATETIME     NOT NULL,
    cidade_id INTEGER      REFERENCES cidades (cidade_id)
                           NOT NULL,
    hora      TIME         NOT NULL,
    duracao   INTEGER      NOT NULL,
    medida    VARCHAR (20) NOT NULL,
    dist_name VARCHAR (20),
    params    VARCHAR (50),
    PRIMARY KEY (
        rota_id,
        saida,
        cidade_id,
        mes,
        dia,
        hora,
        medida
    )
);

CREATE TABLE IF NOT EXISTS simulacoes (
    rota_id     INTEGER  REFERENCES rotas (rota_id),
    saida       DATETIME NOT NULL,
    cidade_id   INTEGER  NOT NULL,
    duracao,    INTEGER,
    cenario     INTEGER,
    temperatura NUMERIC,
    umidade     NUMERIC
);

CREATE TABLE IF NOT EXISTS resultados (
    rota_id INTEGER  REFERENCES rotas (rota_id),
    saida   DATETIME,
    score   NUMERIC,
    PRIMARY KEY (
        rota_id,
        saida
    )
);
