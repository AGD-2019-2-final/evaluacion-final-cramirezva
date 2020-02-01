
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 6
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:CHARARRAY, f3:MAP[]);
    
columna = FOREACH u GENERATE $2;
dato1 = FOREACH columna GENERATE FLATTEN($0);
agrupar = GROUP dato1 BY $0;
contar = FOREACH agrupar GENERATE group, COUNT($1) as ld1:CHARARRAY;

contar1 = FOREACH contar GENERATE CONCAT(group, ',' , ld1);

DUMP contar1;

-- escribe el archivo de salida
STORE contar1 INTO 'output';

-- copia los archivos del HDFS al sistema local
fs -get output/ .
