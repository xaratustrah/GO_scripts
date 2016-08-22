#!/bin/bash

# downloading putty components
addr="http://the.earth.li/~sgtatham/putty/latest/x86/"
execs=("pscp" "plink" "putty" "puttygen" "pageant")

for exec in ${execs[@]} 
do
    echo "Getting ${addr}${exec}.exe"
    curl -sSLO "${addr}${exec}.exe"
done

#downloading python
python_addr="https://www.python.org/ftp/python/3.4.1/"
python_exec="python-3.4.1"
echo "Getting ${python_addr}${python_exec}.msi"
curl -sSLO "${python_addr}${python_exec}.msi"
