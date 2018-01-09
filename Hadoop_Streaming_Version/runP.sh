#!/bin/sh

set -x
source ~/.bash_profile

streaming_jar=/usr/share/hadoop/contrib/streaming/hadoop-streaming-1.0.1.jar

hadoop fs -rmr /user/colinzhang/prj2/temp
hadoop fs -rmr /user/colinzhang/prj2/output
hadoop jar ${streaming_jar} \
	-input "/user/colinzhang/prj2/input/" \
	-output "/user/colinzhang/prj2/temp/" \
	-jobconf mapred.job.map.capacity=$1 \
	-jobconf mapred.job.reduce.capacity=$2 \
	-file "mapper.py" \
	-file "reducer.py" \
	-mapper "python mapper.py" \
	-combiner "python reducer.py" \
	-reducer "python reducer.py" || exit -1

hadoop jar ${streaming_jar} \
	-input "/user/colinzhang/prj2/temp/" \
	-output "/user/colinzhang/prj2/output/" \
	-jobconf mapred.job.map.capacity=$1 \
	-jobconf mapred.job.reduce.capacity=$2 \
	-file "mapper2.py" \
	-file "reducer2.py" \
	-mapper "python mapper2.py" \
	-reducer "python reducer2.py" || exit -1

#hadoop fs -cat '/user/colinzhang/prj2/output/part-00000' || exit -1

exit 0
