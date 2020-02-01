
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 3
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:CHARARRAY, f3:INT);
y = ORDER u BY $2;
r = FOREACH y GENERATE $2;
c = LIMIT r 5;
DUMP c;

-- escribe el archivo de salida
STORE c INTO 'output';

-- copia los archivos del HDFS al sistema local
fs -get output/ .
