-- Pregunta
-- ===========================================================================
-- 
-- Para el archivo `data.tsv` genere una tabla que contenga la primera columna,
-- la cantidad de elementos en la columna 2 y la cantidad de elementos en la 
-- columna 3 separados por coma. La tabla debe estar ordenada por las 
-- columnas 1, 2 y 3.
-- 
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
--
-- >>> Escriba su respuesta a partir de este punto <<<
u = LOAD 'data.tsv'
    AS (f1:CHARARRAY, f2:BAG{}, f3:MAP[]);
    
r = FOREACH u GENERATE $0, COUNT_STAR($1) as ld1:CHARARRAY, SIZE($2) as ld2:CHARARRAY;

rr = FOREACH r GENERATE CONCAT($0, ',' , ld1 , ',' , ld2) as final;

y = ORDER rr BY final;

STORE y INTO './output' using PigStorage('\t');