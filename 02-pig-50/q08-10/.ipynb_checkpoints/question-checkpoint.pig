-- Pregunta
-- ===========================================================================
-- 
-- Para el archivo `data.tsv` compute la cantidad de registros por letra de la 
-- columna 2 y clave de al columna 3; esto es, por ejemplo, la cantidad de 
-- registros en tienen la letra `b` en la columna 2 y la clave `jjj` en la 
-- columna 3 es:
-- 
--   ((b,jjj), 216)
-- 
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
--
-- >>> Escriba su respuesta a partir de este punto <<<
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:BAG{}, f3:MAP[]);

r = FOREACH u GENERATE FLATTEN(f2), FLATTEN(f3);

y = GROUP r BY ($0, $1);

z = FOREACH y GENERATE group,  COUNT(r);

STORE z INTO './output' using PigStorage('\t');
