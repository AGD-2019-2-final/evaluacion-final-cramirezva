#!/usr/bin/env python
import sys

if __name__ == '__main__':
    ## cada linea de texto recibida es una
    ## entrada clave \tabulador valor
    z=[]
    a=[]
    b=[]
    c=[]
    d=[]
    f=[]
    g=[]
    h=[]
    i=[]
    x=['A','B','C','D','F','G','H','I']    
    for line in sys.stdin:
        line = line.strip()
        letras = line.split(',')
        if line.count('A'):        
          a.append(int(letras[0]))
        if line.count('B'):
          b.append(int(letras[0]))
        if line.count('C'):
          c.append(int(letras[0]))
        if line.count('D'):
          d.append(int(letras[0]))
        if line.count('F'):
          f.append(int(letras[0]))
        if line.count('G'):
          g.append(int(letras[0]))
        if line.count('H'):
          h.append(int(letras[0]))
        if line.count('I'):
          i.append(int(letras[0]))
    a = [str(w) for w in sorted(a)]
    b = [str(w) for w in sorted(b)]
    c = [str(w) for w in sorted(c)]
    d = [str(w) for w in sorted(d)]
    f = [str(w) for w in sorted(f)]
    g = [str(w) for w in sorted(g)]
    h = [str(w) for w in sorted(h)]
    i = [str(w) for w in sorted(i)]
    z.extend([a,b,c,d,f,g,h,i])
    
    for i in range(8):
      sys.stdout.write("{}       {}\n".format(x[i],",".join(z[i])))
