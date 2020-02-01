#!/usr/bin/env python
import sys

if __name__ == '__main__':
    ## cada linea de texto recibida es una
    ## entrada clave \tabulador valor
    b=[]
    
    for line in sys.stdin:
        line = line.strip()
        letra, fecha, valor = line.split(',')
        valor=int(valor)
        b.append((letra,fecha,valor))
        

       ## sys.stdout.write("{}\n".format(sorted(line)))
        
        b.sort(key=lambda b: b[2])
        
    
    for i in b[0:6]:
        
    ##   print(i[0]+','+str(i[1]))
       sys.stdout.write("{}\n".format((i[0]+'\t'+ i[1]+'\t'+ str(i[2]))))
