#!/usr/bin/env python
import sys
## Esta funcion reduce los elementos que
## tienen la misma clave
if __name__ == '__main__':

    letra_1={}
    total = 0
    ## cada linea de texto recibida es una
    ## entrada clave \tabulador valor
    for line in sys.stdin:
        line = line.strip()
        letra, valor = line.split(',')
        valor=float(valor)
        if letra in letra_1:
          letra_1[letra].append(valor)
        else:
            letra_1[letra]=[]
            letra_1[letra].append(valor)
    for letra in letra_1.keys():
        maximo=max(letra_1[letra])
        minimo=min(letra_1[letra])
    
        sys.stdout.write("{}\t{}\t{}\n".format(letra, maximo, minimo))
