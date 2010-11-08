#!/bin/zsh -G
mkdir -p obj
rm -f obj/**/*.class && javac GetSortedBAMHeader.java -Xlint:all -d obj && jar cvf GetSortedBAMHeader.jar -C obj .
