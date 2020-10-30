#!/usr/bin/env python
import sys

current_key = None
values = []

def check_intersection(str1, str2):
    set1 = set(str1.split(" "))
    set2 = set(str2.split(" "))
    intersection = sorted(set1.intersection(set2))
    return intersection

def print_result(key, values):
    for i in range(len(values)):
        intersection = check_intersection(values[i], values[-1])
        if(len(intersection)>1 and i != len(values) - 1):
             print('{} {}'.format(key, ' '.join(intersection)))

for line in sys.stdin:
    line = line.strip()
    key, value = line.split(' ',1)
    if(current_key==key):
        values.append(value)
    else:
        if(current_key is None):
            current_key=key
            values.append(value)
        else:
            while len(values)>1:
                print_result(current_key, values)
                values.pop()
            print('{} {}'.format(current_key, current_key))
            values.clear()
            current_key=key
            values.append(value)

while len(values)>1:
    print_result(current_key, values)
    values.pop()
print('{} {}'.format(current_key, current_key))
values.clear()
