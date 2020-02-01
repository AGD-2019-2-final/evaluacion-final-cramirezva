
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 4
u = LOAD 'truck_event_text_partition.csv' USING PigStorage(',')
    AS (f1:INT, f2:INT, f3:CHARARRAY, f4:CHARARRAY, f5:DOUBLE, f6:DOUBLE, f7:CHARARRAY, f8:CHARARRAY, f9:CHARARRAY, f10:INT, f11:CHARARRAY, f12:CHARARRAY);
r = FOREACH u GENERATE CONCAT(f1,',', f2, ',', f3) as ld;
s = LIMIT r 10;
y = ORDER s BY ld;
DUMP y;


-- escribe el archivo de salida
STORE y INTO 'output';

-- copia los archivos del HDFS al sistema local
fs -get output/ .
