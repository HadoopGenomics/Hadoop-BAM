#!/bin/zsh -G
rm -f obj/**/*.class && javac Sort.java -Xlint:all -d obj && jar cvf sorter.jar -C obj .
