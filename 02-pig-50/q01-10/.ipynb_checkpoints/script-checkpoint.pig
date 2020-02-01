
-- crea la carpeta input in el HDFS
fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
u = LOAD 'data.tsv' USING PigStorage('\t')
    AS (f1:CHARARRAY, f2:CHARARRAY, f3:INT);
y = GROUP u BY $0;
z = FOREACH y GENERATE group, COUNT(u);

-- escribe el archivo de salida
STORE z INTO 'output';

-- copia los archivos del HDFS al sistema local
fs -get output/ .
