-- 
-- Pregunta
-- ===========================================================================
--
-- Escriba una consulta que compute la cantidad de registros por letra de la 
-- columna 2 y clave de la columna 3; esto es, por ejemplo, la cantidad de 
-- registros en tienen la letra `a` en la columna 2 y la clave `aaa` en la 
-- columna 3 es:
--
--     a    aaa    5
--
-- Escriba el resultado a la carpeta `output` de directorio de trabajo.
--
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (
    c1 STRING,
    c2 ARRAY<CHAR(1)>, 
    c3 MAP<STRING, INT>
    )
    ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY '#'
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE t0;
--
-- >>> Escriba su respuesta a partir de este punto <<<
--

DROP TABLE IF EXISTS temporal;
CREATE TABLE temporal AS
SELECT  c2 as c2, c3 as c3 FROM t0
;

DROP TABLE IF EXISTS temporal1;
CREATE TABLE temporal1 AS
SELECT  c2, temp1 as temp1 FROM temporal
    LATERAL VIEW explode(c3) ss as temp1, temp2;

DROP TABLE IF EXISTS temporal2;
CREATE TABLE temporal2 AS
SELECT  temp2 as temp2, temp1 FROM temporal1
    LATERAL VIEW explode(c2) sk as temp2;

DROP TABLE IF EXISTS temporal3;
CREATE TABLE temporal3 AS
SELECT  temp2, temp1, count(*) FROM temporal2
    GROUP BY temp2, temp1;


INSERT OVERWRITE LOCAL DIRECTORY 'output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
STORED AS TEXTFILE
Select * FROM temporal3;