#!/bin/sh
mkdir -p obj
javac Summarize.java -Xlint:all,-deprecation -d obj && jar cvf summarizer.jar -C obj .
