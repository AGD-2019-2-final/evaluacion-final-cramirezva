
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 12

u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6:INT);
    
y = FOREACH u GENERATE $2;

f = FILTER y BY ($0 matches '.*(D|E|F|G|H|I|J|K).*');

DUMP f;

-- escribe el archivo de salida
STORE f INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
