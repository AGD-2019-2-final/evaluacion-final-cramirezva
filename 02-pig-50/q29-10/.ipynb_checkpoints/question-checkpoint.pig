-- 
-- Pregunta
-- ===========================================================================
-- 
-- Para responder la pregunta use el archivo `data.csv`.
-- 
-- Escriba el código en Pig para manipulación de fechas que genere la siguiente 
-- salida.
-- 
--    1971-07-08,jul,07,7
--    1974-05-23,may,05,5
--    1973-04-22,abr,04,4
--    1975-01-29,ene,01,1
--    1974-07-03,jul,07,7
--    1974-10-18,oct,10,10
--    1970-10-05,oct,10,10
--    1969-02-24,feb,02,2
--    1974-10-17,oct,10,10
--    1975-02-28,feb,02,2
--    1969-12-07,dic,12,12
--    1973-12-24,dic,12,12
--    1970-08-27,ago,08,8
--    1972-12-12,dic,12,12
--    1970-07-01,jul,07,7
--    1974-02-11,feb,02,2
--    1973-04-01,abr,04,4
--    1973-04-29,abr,04,4
-- 
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
-- 
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

STORE cases INTO './output' using PigStorage('\t');