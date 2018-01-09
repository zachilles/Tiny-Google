import sys

for line in sys.stdin:
	key, freq = line.strip('\n').split("\t")
	filename, w = key.split('##')
	
	print '%s\t%s\t%s' % (w, freq, filename)
