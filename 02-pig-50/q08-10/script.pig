
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 8
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:BAG{}, f3:MAP[]);

r = FOREACH u GENERATE FLATTEN(f2), FLATTEN(f3);

y = GROUP r BY ($0, $1);

z = FOREACH y GENERATE group,  COUNT(r);

DUMP z;

-- escribe el archivo de salida
STORE z INTO 'output';

-- copia los archivos del HDFS al sistema local
fs -get output/ .
