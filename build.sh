#!/bin/zsh -G
rm -f obj/**/*.class && javac BAMSort.java -Xlint:all,-deprecation -d obj && jar cvf sorter.jar -C obj .
