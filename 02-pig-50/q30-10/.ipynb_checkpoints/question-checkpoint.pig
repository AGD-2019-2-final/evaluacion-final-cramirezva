-- 
-- Pregunta
-- ===========================================================================
-- 
-- Para responder la pregunta use el archivo `data.csv`.
-- 
-- Escriba el codigo en Pig para manipulación de fechas que genere la siguiente
-- salida.
-- 
--    1971-07-08,08,8,jue,jueves
--    1974-05-23,23,23,jue,jueves
--    1973-04-22,22,22,dom,domingo
--    1975-01-29,29,29,mie,miercoles
--    1974-07-03,03,3,mie,miercoles
--    1974-10-18,18,18,vie,viernes
--    1970-10-05,05,5,lun,lunes
--    1969-02-24,24,24,lun,lunes
--    1974-10-17,17,17,jue,jueves
--    1975-02-28,28,28,vie,viernes
--    1969-12-07,07,7,dom,domingo
--    1973-12-24,24,24,lun,lunes
--    1970-08-27,27,27,jue,jueves
--    1972-12-12,12,12,mar,martes
--    1970-07-01,01,1,mie,miercoles
--    1974-02-11,11,11,lun,lunes
--    1973-04-01,01,1,dom,domingo
--    1973-04-29,29,29,dom,domingo
-- 
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
--
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

STORE cases INTO './output' using PigStorage('\t');