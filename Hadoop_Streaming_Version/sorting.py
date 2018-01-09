import sys

def comp(a, b):
	if a[0] != b[0]:
		return -cmp(a[0], b[0])
	else:
		return -cmp(a[1], b[1])

result = {}
for line in sys.stdin:
	key, vals = line.strip('\n').split('\t')
	fs = vals.split(',')
	for f in fs:
		fid, freq = f.split(':')
		if fid in result:
			result[fid][0] += 1
		else:
			result[fid] = [1, 0]
		result[fid][1] += int(freq)

flat = result.items()
flat.sort(lambda x, y: comp(x[1], y[1]))

for k, v in flat:
	print('%s\t%s\t%s' % (k, v[0], v[1]))

