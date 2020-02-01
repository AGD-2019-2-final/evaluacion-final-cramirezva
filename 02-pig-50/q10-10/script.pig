
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 10

u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:DATETIME, f5:CHARARRAY, f6:INT);
    
y = GROUP u BY $2;

z = FOREACH y GENERATE group, SIZE($0) as ld1:CHARARRAY;
    
zz = FOREACH z GENERATE CONCAT(group, ',' , ld1) as final:INT;

y = ORDER zz BY final DESC;

w = STREAM y THROUGH  `head -n 5 `;

DUMP w;

-- escribe el archivo de salida
STORE w INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
