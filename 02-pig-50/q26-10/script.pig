
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 26 

u = LOAD 'data.csv' USING PigStorage(',') AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6: INT);

y = FILTER u BY (f2 matches '.*[M-Z].*');

b = FOREACH y GENERATE f2;

DUMP b;

-- escribe el archivo de salida
STORE b INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
