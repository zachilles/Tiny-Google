#!/bin/sh

set -x
source ~/.bash_profile

rm -r -f class
mkdir class
javac -classpath /usr/share/hadoop/hadoop-core-1.0.1.jar -d class src/*.java
jar cvf indexing.jar -C class/ .

exit 0
