import os
import sys

ws = []
keywords = sys.argv[1]
keywords = keywords.lower()
temp = ""
for c in keywords:
	if (c >= 'a' and c <= 'z' or c >= 'A' and c <= 'Z' or c >= '0' and c <= '9'):
		temp = temp + c
	else:
		if (len(temp) > 0) :
			ws.append(temp)
		temp = ""
if (len(temp) > 0):
	ws.append(temp)


for line in sys.stdin:
	key, value = line.strip('\n').split('\t')

	if key in ws:
		print '%s\t%s' %(key, value)

