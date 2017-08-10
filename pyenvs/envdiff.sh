#!/bin/bash
conda env export > jupyter-hpc_pyenv.txt &&  python envdiff.py jupyter-hpc_pyenv.txt cori_pyenv.yml | awk -f coldiff.awk
