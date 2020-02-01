
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 21
u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6:INT);

f = FILTER u BY (f5 matches '.*(blue|green).*');

y = FOREACH f GENERATE f2, f5;

DUMP y;






--Punto 20

--u = LOAD 'data.csv' USING PigStorage(',')
    --AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6:INT);
    
--y = FILTER u BY (NOT f5 matches '.*b.*');

--b = FOREACH y GENERATE CONCAT(f2, ',' , f5);

--DUMP b;

-- escribe el archivo de salida
STORE y INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
