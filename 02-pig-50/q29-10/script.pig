
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 29

u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6: INT);

    
substringdata = FOREACH u GENERATE f4, SUBSTRING (f4, 5, 7);

cases = FOREACH substringdata GENERATE CONCAT($0, ',' , ($1 == '01' ? 'ene' : 
                         ($1 == '02' ? 'feb' :
                         ($1 == '03' ? 'mar' :
                         ($1 == '04' ? 'abr' :
                         ($1 == '05' ? 'may' :
                         ($1 == '06' ? 'jun' :
                         ($1 == '07' ? 'jul' :
                         ($1 == '08' ? 'ago' :
                         ($1 == '09' ? 'sep' :
                         ($1 == '10' ? 'oct' : 
                         ($1 == '11' ? 'nov' : 'dic'))))))))))), ',' , SUBSTRING ($0, 5, 7), ',' , ($1 == '01' ? '1' : 
                         ($1 == '02' ? '2' :
                         ($1 == '03' ? '3' :
                         ($1 == '04' ? '4' :
                         ($1 == '05' ? '5' :
                         ($1 == '06' ? '6' :
                         ($1 == '07' ? '7' :
                         ($1 == '08' ? '8' :
                         ($1 == '09' ? '9' :
                         ($1 == '10' ? '10' : 
                         ($1 == '11' ? '11' : '12'))))))))))));
DUMP cases;

-- escribe el archivo de salida
STORE cases INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
