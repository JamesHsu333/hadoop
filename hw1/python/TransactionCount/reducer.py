#!/usr/bin/env python
import sys

output = set()
last_key = None

def CheckDuplicate(output):
    outputs = sorted(output)
    for value in outputs:
        print('{value}'.format(value=value))
    output.clear()
    outputs.clear()

for line in sys.stdin:
    line = line.strip()
    key, value = line.split(' ',1)
    if(last_key == key):
        if(line not in output):
            output.add(line)
    else: 
        if(last_key):
            CheckDuplicate(output)
            output.add(line)
            last_key = key
        else:
            last_key = key
            output.add(line)

CheckDuplicate(output)
