#!/bin/sh

set -x
source ~/.bash_profile

inputFolder="/user/colinzhang/prj2/smallinput"
tempFolder="/user/colinzhang/prj2/temp"
outputFolder="/user/colinzhang/prj2/output"
indexingFolder="/user/colinzhang/prj2/indexing"

hadoop fs -rmr /user/colinzhang/prj2/temp
hadoop fs -rmr /user/colinzhang/prj2/output

if [ -z "$2" ]
  then
    hadoop jar indexing.jar Indexing $inputFolder $tempFolder $outputFolder
  else
  	hadoop jar indexing.jar Indexing $2 $tempFolder $outputFolder
fi

hadoop fs -cp $outputFolder/part-00000 $indexingFolder/$1part-00000
hadoop fs -cat $outputFolder'/part-00000' || exit -1

exit 0
