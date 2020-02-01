
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 25

u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6: INT);
    
indexof_data = FOREACH u GENERATE INDEXOF(f2, 'a', 1);

DUMP indexof_data;

-- escribe el archivo de salida
STORE indexof_data INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
