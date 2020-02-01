-- 
-- Pregunta
-- ===========================================================================
--
-- Para resolver esta pregunta use el archivo `data.tsv`.
--
-- Construya una consulta que ordene la tabla por letra y valor (3ra columna).
--
-- Escriba el resultado a la carpeta `output` de directorio de trabajo.
--
-- >>> Escriba su respuesta a partir de este punto <<<
--
DROP TABLE IF EXISTS data_tsv_2;

CREATE TABLE data_tsv_2 (letra    STRING,
                       fecha    STRING,
                       cantidad INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH "data.tsv" OVERWRITE INTO TABLE data_tsv_2;


INSERT OVERWRITE LOCAL DIRECTORY 'output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

SELECT * FROM data_tsv_2
ORDER BY letra, cantidad;