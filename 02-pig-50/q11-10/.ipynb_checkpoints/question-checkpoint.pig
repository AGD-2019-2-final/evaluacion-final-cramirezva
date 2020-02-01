-- Pregunta
-- ===========================================================================
-- 
-- Para responder la pregunta use el archivo `data.csv`.
-- 
-- Escriba el código que genere la siguiente salida.
-- 
--    Boyer,BOYER,boyer
--    Coffey,COFFEY,coffey
--    Conway,CONWAY,conway
--    Crane,CRANE,crane
--    Diaz,DIAZ,diaz
--    Estes,ESTES,estes
--    Fry,FRY,fry
--    Garrett,GARRETT,garrett
--    Guy,GUY,guy
--    Hamilton,HAMILTON,hamilton
--    Holcomb,HOLCOMB,holcomb
--    Jarvis,JARVIS,jarvis
--    Kinney,KINNEY,kinney
--    Klein,KLEIN,klein
--    Knight,KNIGHT,knight
--    Noel,NOEL,noel
--    Sexton,SEXTON,sexton
--    Silva,SILVA,silva
-- 
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
--
-- >>> Escriba su respuesta a partir de este punto <<<
--
--Punto 11
    
u = LOAD 'data.csv' USING PigStorage(',')
    AS (f1:INT, f2:CHARARRAY, f3:CHARARRAY, f4:CHARARRAY, f5:CHARARRAY, f6:INT);
    
y = FOREACH u GENERATE CONCAT($2, ',' , UPPER($2), ',' , LOWER($2));

o = ORDER y BY $0;

STORE o INTO './output' using PigStorage('\t');
