#!/bin/sh
javac Sort.java -Xlint:all -d obj && jar cvf sorter.jar -C obj .
