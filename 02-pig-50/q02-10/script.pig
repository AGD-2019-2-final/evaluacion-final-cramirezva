
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:CHARARRAY, f3:INT);
y = ORDER u BY $0,$2;
DUMP y;   

-- escribe el archivo de salida
STORE y INTO 'output';

-- copia los archivos del HDFS al sistema local
fs -get output/ .
