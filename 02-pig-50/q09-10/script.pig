
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 9
u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6:INT);
c = FOREACH u GENERATE CONCAT ($1, '@', $2);
DUMP c;

-- escribe el archivo de salida
STORE c INTO 'output';

-- copia los archivos del HDFS al sistema local
fs -get output/ .
