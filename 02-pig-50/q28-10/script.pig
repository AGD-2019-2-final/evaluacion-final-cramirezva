
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 28
u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6: INT);

substringdata = FOREACH u GENERATE SUBSTRING (f4, 0, 4) as ld1, SUBSTRING (f4, 2, 4) as ld2;

substringdata2 = FOREACH substringdata GENERATE CONCAT(ld1 , ',' , ld2);

DUMP substringdata2;

-- escribe el archivo de salida
STORE substringdata2 INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
