#!/bin/sh

set -x
source ~/.bash_profile

streaming_jar=/usr/share/hadoop/contrib/streaming/hadoop-streaming-1.0.1.jar

hadoop fs -rmr /user/colinzhang/prj2/result
hadoop jar ${streaming_jar} \
	-input "/user/colinzhang/prj2/output/" \
	-output "/user/colinzhang/prj2/result/" \
	-file "sorting.py" \
	-file "sortingmapper.py" \
	-mapper "python sortingmapper.py \"$1\"" \
	-reducer "python sorting.py" || exit -1
#-mapper "grep -E \"^($1)	\"" \
hadoop fs -cat '/user/colinzhang/prj2/result/part-00000' | head -10 || exit -1

exit 0
