#!/bin/sh

set -x
source ~/.bash_profile

indexingFolder="/user/colinzhang/prj2/indexing"
resultFolder="/user/colinzhang/prj2/result"

hadoop fs -rmr $resultFolder

if [ -z "$1" ]
  then
    hadoop jar indexing.jar Searching "hadoop world" $indexingFolder $resultFolder
  else
  	hadoop jar indexing.jar Searching $1 $indexingFolder $resultFolder
fi




hadoop fs -cat $resultFolder'/part-00000' | head -10 || exit -1

exit 0