CREATE VIEW IF NOT EXISTS aderencia AS
    SELECT DISTINCT medida,
                    dist_name AS distribuicao,
                    count(1) OVER (PARTITION BY dist_name,
                    medida) AS frequencia_absoluta,
                    round(100 * count(1) OVER (PARTITION BY dist_name,
                                medida) / CAST (count(1) OVER (PARTITION BY medida) AS FLOAT), 2) AS frequencia_relativa
    FROM distribuicoes
    ORDER BY medida,
             count(1) OVER (PARTITION BY dist_name,
             medida) DESC;