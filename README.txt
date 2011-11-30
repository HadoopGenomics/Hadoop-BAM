Hadoop-BAM: a library for manipulation of BAM (Binary Alignment/Map) and
BGZF-compressed files using the Hadoop MapReduce framework, and command line
tools in the vein of SAMtools.

------------
Dependencies
------------

Hadoop 0.20.2 or 0.20.203.0. 0.21 or later will not work with this version of
Hadoop-BAM.

Picard SAM-JDK. Version 1.56 is provided in the form of sam-1.56.jar and
picard-1.56.jar. Later versions may also work but have not been tested.

Availability:
	Hadoop - http://hadoop.apache.org/
	Picard - http://picard.sourceforge.net/

In order to use all the functionality of Hadoop-BAM, you need to have Picard's
"sam-1.56.jar" and "picard-1.56.jar" (assuming version 1.56) and Hadoop's
"hadoop-0.20.2-core.jar" (assuming version 0.20.2) in the CLASSPATH environment
variable, and, for running under Hadoop, in the HADOOP_CLASSPATH setting in
hadoop-env.sh.

------------
Installation
------------

The easiest way to build Hadoop-BAM is to use Apache Ant (version 1.6 or
greater) with the following command:

	ant jar

This will create the 'hadoop-bam.jar' file. For Javadoc documentation, run:

	ant javadoc

Documentation can then be found in the "doc" subdirectory.

-------------
Library usage
-------------

See the Javadoc as well as the command line plugins' source code (in
src/fi/tkk/ics/hadoop/bam/cli/plugins/*.java) for library usage information. In
particular, for MapReduce usage, see
src/fi/tkk/ics/hadoop/bam/cli/plugins/Sort.java.

------------------
Command-line usage
------------------

Hadoop-BAM can be used as a command-line tool, with functionality in the form
of plugins that provide commands to which hadoop-bam.jar is a frontend.
Hadoop-BAM provides some commands of its own, but any others found in the Java
class path will be used as well.

Running under Hadoop
....................

To use Hadoop-BAM under Hadoop, make sure that, in addition to hadoop-bam.jar,
Picard's "sam-1.56.jar" and "picard-1.56.jar" (assuming version 1.56) have been
added to the HADOOP_CLASSPATH in the Hadoop configuration's hadoop-env.sh,
along with any plugin .jar files that provide other commands. Then, you may run
Hadoop-BAM with a command like:

	hadoop jar hadoop-bam.jar

This should print a brief help message listing the commands available. To run a
command, give it as the first command-line argument. For example, the provided
BAM sorting command, "sort":

	hadoop jar hadoop-bam.jar sort

This will give a help message specific to that command.

File paths under Hadoop
.......................

When running under Hadoop, keep in mind that file paths refer to the
distributed file system, HDFS. To explicitly access a local file, instead of
using the plain path such as "/foo/bar", you must use a file: URI, such as
"file:/foo/bar". Note that paths in file: URIs must be absolute.

Output of MapReduce-using commands
..................................

An example of a MapReduce-using command is "sort". Like all such commands
should, it takes a working directory argument in which to place its output in
parts. Each part is the output of one reduce task. For convenience, a "-o"
parameter is supported to output a single complete BAM file instead of the
individual parts.

Note that some commands, such as the provided "view" and "index" commands, do
not use MapReduce: they are merely useful to operate directly on files stored
in HDFS.

Running without Hadoop
......................

Hadoop-BAM can be run directly, outside Hadoop, as long as it and the Picard
SAM-JDK and Hadoop .jar files ("sam-1.56.jar" and "picard-1.56.jar" and
"hadoop-0.20.2-core.jar" for versions 1.56 and 0.20.2 respectively) are in the
Java class path. A command such as the following:

	java fi.tkk.ics.hadoop.bam.cli.Frontend

Is equivalent to the "hadoop jar hadoop-bam.jar" command used earlier. This has
limited application, but it can be used e.g. for testing purposes.
