
-- crea la carpeta input in el HDFS
-- fs -mkdir input

-- copia los archivos del sistema local al HDFS
fs -put input/ .

-- carga de datos
--Punto 30

u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6: INT);

    
substringdata = FOREACH u GENERATE f4, SUBSTRING (f4, 8, 10), (SUBSTRING (f4, 8, 10) == '01' ? '1' : 
                         (SUBSTRING (f4, 8, 10) == '02' ? '2' :
                         (SUBSTRING (f4, 8, 10) == '03' ? '3' :
                         (SUBSTRING (f4, 8, 10) == '04' ? '4' :
                         (SUBSTRING (f4, 8, 10) == '05' ? '5' :
                         (SUBSTRING (f4, 8, 10) == '06' ? '6' :
                         (SUBSTRING (f4, 8, 10) == '07' ? '7' :
                         (SUBSTRING (f4, 8, 10) == '08' ? '8' :
                         (SUBSTRING (f4, 8, 10) == '09' ? '9' :
                         (SUBSTRING (f4, 8, 10) == '10' ? '10' :
                         (SUBSTRING (f4, 8, 10) == '11' ? '11' :
                         (SUBSTRING (f4, 8, 10) == '12' ? '12' :
                         (SUBSTRING (f4, 8, 10) == '13' ? '13' :
                         (SUBSTRING (f4, 8, 10) == '14' ? '14' :
                         (SUBSTRING (f4, 8, 10) == '15' ? '15' :
                         (SUBSTRING (f4, 8, 10) == '16' ? '16' :
                         (SUBSTRING (f4, 8, 10) == '17' ? '17' :
                         (SUBSTRING (f4, 8, 10) == '18' ? '18' :
                         (SUBSTRING (f4, 8, 10) == '19' ? '19' :
                         (SUBSTRING (f4, 8, 10) == '20' ? '20' :
                         (SUBSTRING (f4, 8, 10) == '21' ? '21' :
                         (SUBSTRING (f4, 8, 10) == '22' ? '22' :
                         (SUBSTRING (f4, 8, 10) == '23' ? '23' :
                         (SUBSTRING (f4, 8, 10) == '24' ? '24' :
                         (SUBSTRING (f4, 8, 10) == '25' ? '25' :
                         (SUBSTRING (f4, 8, 10) == '26' ? '26' :
                         (SUBSTRING (f4, 8, 10) == '27' ? '27' :
                         (SUBSTRING (f4, 8, 10) == '28' ? '28' :
                         (SUBSTRING (f4, 8, 10) == '29' ? '29' : 
                         (SUBSTRING (f4, 8, 10) == '30' ? '30' : '31')))))))))))))))))))))))))))))), ToString( ToDate(f4,'yyyy-MM-dd'), 'EEE' ) ; 

cases = FOREACH substringdata GENERATE CONCAT($0, ',' , $1, ',' , $2, ',' , ($3 == 'Sun' ? 'dom' : 
                         ($3 == 'Mon' ? 'lun' :
                         ($3 == 'Tue' ? 'mar' :
                         ($3 == 'Wed' ? 'mie' :
                         ($3 == 'Thu' ? 'jue' :
                         ($3 == 'Fri' ? 'vie' : 
                         ($3 == 'Sat' ? 'sab' : 'sun'))))))), ',' , ($3 == 'Sun' ? 'domingo' : 
                         ($3 == 'Mon' ? 'lunes' :
                         ($3 == 'Tue' ? 'martes' :
                         ($3 == 'Wed' ? 'miercoles' :
                         ($3 == 'Thu' ? 'jueves' :
                         ($3 == 'Fri' ? 'viernes' :
                         ($3 == 'Sat' ? 'sabado' : 'domingo'))))))));
DUMP cases;


-- escribe el archivo de salida
STORE cases INTO 'output';

-- copia los warchivos del HDFS al sistema local
fs -get output/ .
