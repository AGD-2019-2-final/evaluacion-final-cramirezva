
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 7
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:BAG{}, f3:MAP[]);
    
r = FOREACH u GENERATE $0, COUNT_STAR($1) as ld1:CHARARRAY, SIZE($2) as ld2:CHARARRAY;

--ld1 ={CHARARRAY};
--ld2 ={CHARARRAY};

rr = FOREACH r GENERATE CONCAT($0, ',' , ld1 , ',' , ld2) as final;

y = ORDER rr BY final;
DUMP y;

-- escribe el archivo de salida
STORE y INTO 'output';

-- copia los archivos del HDFS al sistema local
fs -get output/ .
