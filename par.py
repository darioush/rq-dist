

opts0 = ['-G {0}'.format(g) for g in ['file', 'method', 'line']]
opts1 = ['-M {0}'.format(m) for m in ['line:cobertura', 'line', 'branch', 'branch-line', 'mutant', 'mutant-line']]
opts2 = ['-x {0}'.format(x) for x in ['0,B.F',]] #'B,G', '0,G', '0,B.F.G', '0,B.G', 'B,F.G']]
opts4 = ['-v {0}'.format(x) for x in ['1-14', '16-16', '18-19', '21-37', '39-53', '55-58', '60-MAX']]
opts = ['{v} {g} {m} {x}'.format(v=v, g=g,m=m,x=x) for v in opts4 for g in opts0 for m in opts1 for x in opts2]

for opt in opts:
    print opt
