import os
import pandas as pd
import re

pd.set_option('display.width', 0)
pd.set_option('display.max_rows', None)

names = ['cori','oliver-laptop']
files = ['{}_pyenv.yml'.format(name) for name in names]
data = {}
dfs = []

for name, f in zip(names, files):
    with open(f) as fh:
        lines = fh.readlines()

    data_raw = [line.strip() for line in lines if '=' in line]
    data = [re.sub('[- ]','', p).split('=',maxsplit=1) for p in data_raw]

    pkgs, versions = zip(*data)

    # Replace local dir with 'local'
    pkgs = [re.sub('\(.*\)', ' (local)', pkg) for pkg in pkgs] 

    df = pd.DataFrame(
        data=list(versions),
        index=list(pkgs),
        columns=[name]
    )

    dfs.append(df)

df = pd.concat(dfs, axis=1)


for name in names:
    with open('{}_table.txt'.format(name), 'w') as fh:
        fh.write(str(df[name]))


