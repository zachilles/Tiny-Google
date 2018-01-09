import sys

prev_key = None
sum_value = 0

for line in sys.stdin:
	key, value = line.strip('\n').split('\t')
	if key != prev_key:
		if prev_key is not None:
			print '%s\t%s' %(prev_key, sum_value)	
		prev_key, sum_value = key, int(value)
	else:
		sum_value += int(value)
if prev_key is not None:
	print '%s\t%s' %(prev_key, sum_value)	
