-- 
-- Pregunta
-- ===========================================================================
--
-- Escriba una consulta que retorne la columna `tbl0.c1` y el valor 
-- correspondiente de la columna `tbl1.c4` para la columna `tbl0.c2`.
--
-- Escriba el resultado a la carpeta `output` de directorio de trabajo.
--
DROP TABLE IF EXISTS tbl0;
CREATE TABLE tbl0 (
    c1 INT,
    c2 STRING,
    c3 INT,
    c4 DATE,
    c5 ARRAY<CHAR(1)>, 
    c6 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'tbl0.csv' INTO TABLE tbl0;
--
DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (
    c1 INT,
    c2 INT,
    c3 STRING,
    c4 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'tbl1.csv' INTO TABLE tbl1;
--
-- >>> Escriba su respuesta a partir de este punto <<<
--
DROP TABLE IF EXISTS temporal;
CREATE TABLE temporal AS
SELECT  tbl0.c1 as column1, tbl0.c2 as column2, tbl1.c4 as column3
FROM tbl0 JOIN tbl1
ON (tbl0.c1 = tbl1.c1);


DROP TABLE IF EXISTS temporal1;
CREATE TABLE temporal1 AS
SELECT  column1, column2, temp1 as temp1, temp2 as temp2 FROM temporal
    LATERAL VIEW explode(column3) ss as temp1, temp2;



DROP TABLE IF EXISTS temporal2;
CREATE TABLE temporal2 AS
SELECT  column1, column2, temp2 FROM temporal1
WHERE temp1 = column2;
   

INSERT OVERWRITE LOCAL DIRECTORY 'output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
STORED AS TEXTFILE
Select * FROM temporal2;
