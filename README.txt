Hadoop-BAM: a library for manipulation of BAM (Binary Alignment/Map) and
BGZF-compressed files using the Hadoop MapReduce framework, and command line
tools in the vein of SAMtools.

------------
Dependencies
------------

Hadoop 0.20.2 or later in the 0.20 or 1.0 series. The unstable 0.21, 0.22, and
0.23 releases will not work with this version of Hadoop-BAM.

Picard SAM-JDK. Version 1.56 is provided in the form of sam-1.56.jar and
picard-1.56.jar. Later versions may also work but have not been tested.

Availability:
   Hadoop - http://hadoop.apache.org/
   Picard - http://picard.sourceforge.net/

------------
Installation
------------

A precompiled "hadoop-bam.jar" is provided. You may also build it yourself
using the commands below.

The easiest way to compile Hadoop-BAM is to use Apache Ant (version 1.7.1 or
greater) with the following simple command:

   ant

Note that for this to work, either the HADOOP_HOME environment variable should
be set to point to the main Hadoop directory, or the relevant .jar files should
be in the CLASSPATH environment variable. If one wants to use CLASSPATH
directly, it should contain the following .jars:

   - hadoop-0.20.2-core.jar   For Hadoop 0.20.2; the appropriate core .jar file
                              for other releases.

   - sam-1.56.jar             Part of Picard, provided in this Hadoop-BAM
                              distribution; later versions may or may not work.

   - picard-1.56.jar          Ditto.

   - commons-cli-1.2.jar      Apache Commons CLI; provided by Hadoop in its
                              lib/ subdirectory. Hadoop loads it when used so
                              this should never be necessary when using
                              Hadoop-BAM, but it is required in this build
                              step.

The previous command will create the 'hadoop-bam.jar' file. For Javadoc
documentation, run:

   ant javadoc

Documentation can then be found in the "doc" subdirectory.

Finally, to run tests, which require version 4 of the JUnit test framework
(available at http://junit.sourceforge.net/), run:

   ant run-tests

-------------
Library usage
-------------

Hadoop-BAM provides the standard set of Hadoop file format classes for BAM
files: a FileInputFormat and a RecordReader, and a FileOutputFormat and a
RecordWriter. These are BAMInputFormat, BAMRecordReader, BAMOutputFormat, and
BAMRecordWriter, respectively.

Note that Hadoop-BAM is based around the newer Hadoop API introduced in the
0.20 Hadoop releases instead of the older, deprecated API.

See the Javadoc as well as the command line plugins' source code (in
src/fi/tkk/ics/hadoop/bam/cli/plugins/*.java) for more information. In
particular, for MapReduce usage examples, see for example
src/fi/tkk/ics/hadoop/bam/cli/plugins/Sort.java and
src/fi/tkk/ics/hadoop/bam/cli/plugins/chipster/Summarize.java.

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
"hadoop-0.20.2-core.jar" for versions 1.56 and 0.20.2 respectively) as well as
the Apache Commons CLI .jar provided by Hadoop ("lib/commons-cli-1.2.jar" for
version 0.20.2) are in the Java class path. A command such as the following:

	java fi.tkk.ics.hadoop.bam.cli.Frontend

Is equivalent to the "hadoop jar hadoop-bam.jar" command used earlier. This has
limited application, but it can be used e.g. for testing purposes.

------------------
Summarizer plugins
------------------

This part explains some behaviour of the summarizing plugins, available in the
command line interface as "hadoop jar hadoop-bam.jar summarize" and "hadoop jar
hadoop-bam.jar summarysort". Unless you are a Chipster user, this section is
unlikely to be relevant to you, and even then, this is not likely to be
something you are interested in.

Summarization is typically best done with the "hadoop jar hadoop-bam.jar
summarize --sort -o output-directory" command. Then there is no need to worry
about concatenating nor sorting the output, as both are done automatically in
this one command. But if you do not pass the "--sort" option, do remember that
Chipster needs the outputs sorted before it can make use of them. For this, you
need to run a separate "hadoop jar hadoop-bam.jar summarysort" command for each
summary file output by "summarize".

Output format
.............

The summarizer's output format is tabix-compatible. It is composed of rows of
tab-separated data:

	<reference sequence ID>	<left coordinate> <right coordinate> <count>

The coordinate columns are 1-based and both ends are inclusive.

The 'count' field represents the number of alignments that have been summarized
into that single range. Note that it may not exactly match any of the 'level'
arguments passed to Summarize, due to Hadoop splitting the file at a boundary
which is not an even multiple of the requested level.

Note that the output files are BGZF-compressed, but do not include the empty
terminating block which would make them valid BGZF files. This is to avoid
having to remove it from the end of each output file in distributed usage (when
not using the "-o" convenience parameter): it's much simpler to put an empty
gzip block to the end of the output.


Unit tests
--------------

Unit tests exists for some of the classes in hadoop-bam.  To build and run
them you'll need JUnit4.  Run

  ant run-tests

from the hadoop-bam base directory.
