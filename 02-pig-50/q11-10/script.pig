
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 11

u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6:INT);
    
y = FOREACH u GENERATE CONCAT($2, ',' , UPPER($2), ',' , LOWER($2));

o = ORDER y BY $0;

DUMP o;

-- escribe el archivo de salida
STORE o INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
