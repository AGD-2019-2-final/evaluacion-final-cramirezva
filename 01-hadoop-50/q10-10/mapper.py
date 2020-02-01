#! /usr/bin/env python

import sys
if __name__ == "__main__":

    # input comes from STDIN (standard input)
    for line in sys.stdin:
        line = line.strip()
        line = line.split('\t')

        if len(line) >=2:
            letra = line[0]
            fecha = line[1]
            

            sys.stdout.write("{},{}\n".format(letra, fecha))
        ##    print ('%s\t%s' % (purpose, amount))
