-- Pregunta
-- ===========================================================================
-- 
-- Obtenga los cinco (5) valores más pequeños de la 3ra columna.
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
--
-- >>> Escriba su respuesta a partir de este punto <<<
--Punto 3
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:CHARARRAY, f3:INT);
y = ORDER u BY $2;
r = FOREACH y GENERATE $2;
c = LIMIT r 5;
STORE c INTO './output' using PigStorage('\t');
