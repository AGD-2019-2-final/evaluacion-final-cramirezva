#! /usr/bin/env python

import sys
if __name__ == "__main__":

    # input comes from STDIN (standard input)
    for line in sys.stdin:
        line = line.strip()
        line = line.split('  ')

        if len(line) >=2:
            num_mes = line[1]
            mes = num_mes.split('-')
            mes = mes[1]

            sys.stdout.write("{}\t1\n".format(mes))
