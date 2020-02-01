#! /usr/bin/env python

import sys
if __name__ == "__main__":

    # input comes from STDIN (standard input)
    for line in sys.stdin:
        line = line.strip()
        line = line.split('   ')

        if len(line) >=2:
            letra = line[0]
            valor = line[2]            

            sys.stdout.write("{},{}\n".format(letra, valor))
        ##    print ('%s\t%s' % (purpose, amount))
