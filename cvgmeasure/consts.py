import os
from plumbum import LocalPath

ABS_PATH = LocalPath(os.path.abspath(__file__)).dirname
JAR_PATH = str(LocalPath(ABS_PATH) /  'tgLister.jar')

LINES      = ['line:'      + T for T in ('cobertura', 'codecover', 'jmockit', 'major')]
BRANCHES   = ['branch:'    + T for T in ('cobertura', 'codecover', 'jmockit')]
TERMS      = ['term:'      + T for T in ('cobertura', 'codecover', 'jmockit')]
STATEMENTS = ['statement:' + T for T in ('codecover',)]
LOOPS      = ['loop:'      + T for T in ('codecover', )]
DATA       = ['data:'      + T for T in ('jmockit', )]
PATHS      = ['path:'      + T for T in ('jmockit',)]
MUTANTS    = ['mutant:'    + T for T in ('major', )]
MUTCVGS    = ['mutcvg:'    + T for T in ('major', )]
ALL_TGS    = LINES + BRANCHES + STATEMENTS + LOOPS + DATA + PATHS + MUTANTS + MUTCVGS # + TERMS
