-- 
-- Pregunta
-- ===========================================================================
-- 
-- Para el archivo `data.tsv` Calcule la cantidad de registros por clave de la 
-- columna 3. En otras palabras, cuántos registros hay que tengan la clave 
-- `aaa`?
-- 
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
--
-- >>> Escriba su respuesta a partir de este punto <<<
--Punto 6
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:CHARARRAY, f3:MAP[]);
    
columna = FOREACH u GENERATE $2;
dato1 = FOREACH columna GENERATE FLATTEN($0);
agrupar = GROUP dato1 BY $0;
contar = FOREACH agrupar GENERATE group, COUNT($1) as ld1:CHARARRAY;
contar1 = FOREACH contar GENERATE CONCAT(group, ',' , ld1);
STORE contar1 INTO './output' using PigStorage('\t');