
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 15

u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6:INT);
    
y = FILTER u BY (f5 matches '.*blue.*') AND (f2 matches '.*Z.*');

b = FOREACH y GENERATE f2, f5;

DUMP b;

-- escribe el archivo de salida
STORE b INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
