SELECT
    plataforma,
    round(AVG(avaliacao),2) AS media_produtos
FROM 
    "zoop-glue-redes_sociais_zoop_silver"
GROUP BY
    plataforma
ORDER BY
    media_produtos;