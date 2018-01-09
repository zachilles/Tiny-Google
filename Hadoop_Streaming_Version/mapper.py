import os
import sys

filename = os.path.split(os.environ.get('map_input_file', 'LOCAL'))[1]

for line in sys.stdin:
	line = line.strip('\n').lower()
	ws = []

	temp = ""
	for c in line:
		if (c >= 'a' and c <= 'z' or c >= 'A' and c <= 'Z' or c >= '0' and c <= '9'):
			temp = temp + c
		else:
			if (len(temp) > 0) :
				ws.append(temp)
			temp = ""
	if (len(temp) > 0) :
		ws.append(temp)

	for w in ws:
		print '%s##%s\t1' %(filename, w)
