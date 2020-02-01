-- 
-- Pregunta
-- ===========================================================================
--
-- Para resolver esta pregunta use el archivo `data.tsv`.
--
-- Compute la cantidad de registros por cada letra de la columna 1.
-- Escriba el resultado ordenado por letra. 
--
-- Escriba el resultado a la carpeta `output` de directorio de trabajo.
--
-- >>> Escriba su respuesta a partir de este punto <<<
--
CREATE DATABASE IF NOT EXISTS data_tsv;

CREATE TABLE data_tsv (letra    STRING,
                       fecha    STRING,
                       cantidad INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH "data.tsv" OVERWRITE INTO TABLE data_tsv;


INSERT OVERWRITE LOCAL DIRECTORY 'output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

SELECT letra, COUNT(*) FROM data_tsv
GROUP BY letra;
