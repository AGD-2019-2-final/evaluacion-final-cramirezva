-- Pregunta
-- ===========================================================================
-- 
-- Para responder la pregunta use el archivo `data.csv`.
-- 
-- Genere una relación con el apellido y su longitud. Ordene por longitud y 
-- por apellido. Obtenga la siguiente salida.
-- 
--   Hamilton,8
--   Garrett,7
--   Holcomb,7
--   Coffey,6
--   Conway,6
-- 
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;

--
-- >>> Escriba su respuesta a partir de este punto <<<
--

--Punto 10
u = LOAD 'data.csv' USING PigStorage(',') AS (id:int, firstname:CHARARRAY, surname:CHARARRAY, birthday:CHARARRAY, color:CHARARRAY,quantity:INT);
x = FOREACH u GENERATE surname as name;
y = FOREACH x GENERATE name as name, SIZE(name) as calc;
w = ORDER y BY calc DESC, name;
z = LIMIT w 5;
STORE z INTO './output' using PigStorage(',');