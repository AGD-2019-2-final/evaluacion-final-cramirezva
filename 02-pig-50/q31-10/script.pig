
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 31
u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6: INT);

substringdata = FOREACH u GENERATE SUBSTRING (f4, 0, 4);

words = FOREACH substringdata GENERATE FLATTEN(TOKENIZE($0)) AS word;

grouped = GROUP words BY word;

wordcount = FOREACH grouped GENERATE group, COUNT(words) as ld1: CHARARRAY;
    
wordcount2 = FOREACH wordcount GENERATE CONCAT(group, ',' , ld1);
    
DUMP wordcount2;


-- escribe el archivo de salida
STORE wordcount2 INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
