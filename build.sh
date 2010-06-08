#!/bin/sh
mkdir -p obj
javac Summarize.java -Xlint:all -d obj && jar cvf summarizer.jar -C obj .
