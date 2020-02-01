#! /usr/bin/env python

import sys
if __name__ == "__main__":

    # input comes from STDIN (standard input)
    for line in sys.stdin:
        line = line.strip()
        line = line.split(',')

        if len(line) >=2:
            credit_history = line[2]

            sys.stdout.write("{}\t1\n".format(credit_history))
