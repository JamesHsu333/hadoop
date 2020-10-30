#!/usr/bin/env python
import sys

for line in sys.stdin:
    line = line.strip()
    value = line.split(" ")
    for key in value:
        print('{} {}'.format(key, line))
