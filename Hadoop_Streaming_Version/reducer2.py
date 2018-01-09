import sys

prev_key = None
file_freq = []

for line in sys.stdin:
	key, freq, filename = line.strip('\n').split('\t')
	if key != prev_key:
		if prev_key is not None:
			file_freq.sort(lambda x, y: -cmp(x[1], y[1]))
			print '%s\t%s' %(prev_key, ','.join(['%s:%s' %(k, v) for k, v in file_freq]))
		prev_key = key
		file_freq = [(filename, int(freq))]
	else:
		file_freq.append((filename, int(freq)))
if prev_key is not None:
	file_freq.sort(lambda x, y: -cmp(x[1], y[1]))
	print '%s\t%s' %(prev_key, ','.join(['%s:%s' %(k, v) for k, v in file_freq]))
