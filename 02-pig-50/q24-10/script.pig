
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 24
u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6: INT);

substringdata = FOREACH u GENERATE SUBSTRING (f4, 5, 7);

DUMP substringdata;

-- escribe el archivo de salida
STORE substringdata INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
