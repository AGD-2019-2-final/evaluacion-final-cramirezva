#!/usr/bin/env python
import sys
## Esta funcion reduce los elementos que
## tienen la misma clave
if __name__ == '__main__':

    purpose_1={}
    total = 0
    ## cada linea de texto recibida es una
    ## entrada clave \tabulador valor
    for line in sys.stdin:
        line = line.strip()
        purpose, amount = line.split(',')
        amount=int(amount)
        if purpose in purpose_1:
          purpose_1[purpose].append(amount)
        else:
            purpose_1[purpose]=[]
            purpose_1[purpose].append(amount)
    for purpose in purpose_1.keys():
        sump=max(purpose_1[purpose])
        sys.stdout.write("{}\t{}\n".format(purpose, sump))
