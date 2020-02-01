
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 5
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:CHARARRAY, f3:CHARARRAY);
columna = FOREACH u GENERATE FLATTEN($1);
dato1 = FOREACH columna GENERATE FLATTEN(TOKENIZE($0)) AS letra;
agrupar = GROUP dato1 BY letra;
contar = FOREACH agrupar GENERATE group, COUNT($1);
limite = LIMIT contar 7;
DUMP limite;


-- escribe el archivo de salida
STORE limite INTO 'output';

-- copia los archivos del HDFS al sistema local
fs -get output/ .
