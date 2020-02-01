-- 
-- Pregunta
-- ===========================================================================
--
-- Para resolver esta pregunta use el archivo `data.tsv`.
--
-- Escriba una consulta que devuelva los cinco valores diferentes más pequeños 
-- de la tercera columna.
--
-- Escriba el resultado a la carpeta `output` de directorio de trabajo.
--
-- >>> Escriba su respuesta a partir de este punto <<<

DROP TABLE IF EXISTS data;

CREATE TABLE data (letra       STRING,
                           fecha        STRING,
                           numero      INT)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE data;

DROP TABLE IF EXISTS salida;
CREATE TABLE salida
AS
SELECT numero 
    FROM (select distinct numero from data order by numero) w LIMIT 5;
