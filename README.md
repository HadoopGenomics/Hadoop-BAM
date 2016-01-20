
  Hadoop-BAM
==============

Hadoop-BAM: a library for the manipulation of files in common bioinformatics
formats using the Hadoop MapReduce framework, and command line tools in the
vein of SAMtools.

Build status
------------
[![Build Status](https://travis-ci.org/HadoopGenomics/Hadoop-BAM.svg?branch=master)](https://travis-ci.org/HadoopGenomics/Hadoop-BAM)


The file formats currently supported are:

   - BAM (Binary Alignment/Map)
   - SAM (Sequence Alignment/Map)
   - FASTQ
   - FASTA (input only)
   - QSEQ
   - VCF (Variant Call Format)
   - BCF (Binary VCF) (output is always BGZF-compressed)

For a longer high-level description of Hadoop-BAM, refer to the article
"Hadoop-BAM: directly manipulating next generation sequencing data in the
cloud" in Bioinformatics Volume 28 Issue 6 pp. 876-877, also available online
at: http://dx.doi.org/10.1093/bioinformatics/bts054

If you are interested in using Apache Pig (http://pig.apache.org/) with
Hadoop-BAM, refer to SeqPig at: http://seqpig.sourceforge.net/

Note that the library part of Hadoop-BAM is primarily intended for developers
with experience in using Hadoop. The command line tools of Hadoop-BAM should be
understandable to all users, but they are limited in scope. SeqPig is a more
versatile and higher-level interface to the file formats supported by
Hadoop-BAM.

For examples of how to use Hadoop-BAM as a library to read data files
in Hadoop see the examples/ directory.

------------
Dependencies
------------

Hadoop. Tested with 1.1.2, 1.2.1 and 2.2.0. Older stable versions as far back
as 0.20.2 should also work. Version 4.2.0 of Cloudera's distribution,
CDH, has also been tested. Use other versions at your own risk. You
can change the version of Hadoop linked against by modifying the
corresponding paramter in the pom.xml build file.

HTSJDK (formerly Picard SAM-JDK) Version 1.133 is required. Later versions
may also work but have not been tested. A version of Picard is distributed
via the unofficial maven repository (see below).

Availability:

   - Hadoop              http://hadoop.apache.org/
   - HTSJDK/Picard       https://github.com/samtools/htsjdk

Note that starting from version 7.4.0 Hadoop-BAM requires Java 8.

------------
Installation
------------

If you're using Hadoop 2.2.0, a precompiled "hadoop-bam-X.Y.Z.jar" is available
that you can use.  Otherwise, you'll have to build Hadoop-BAM yourself by
by using Maven (version 3.0.4 at least) and following the instructions below.

### Hadoop version

You must set the <hadoop.version> tag in pom.xml appropriately for the version
of hadoop you're using.  Run "hadoop version" and copy the string from the output:

    [luca@vm Hadoop-BAM]# hadoop version 2>/dev/null | grep Hadoop
    Hadoop 2.0.0-cdh4.6.0

In the output above, the version string is "2.0.0-cdh4.6.0".

### Build

Build Hadoop-BAM with the following command:

   mvn clean package -DskipTests

It will create two files:

    target/hadoop-bam-X.Y.Z-SNAPSHOT.jar
    target/hadoop-bam-X.Y.Z-SNAPSHOT-jar-with-dependencies.jar

The former contains only Hadoop-BAM whereas the latter one also contains all
dependencies and can be run directly via

    hadoop jar target/hadoop-bam-X.Y.Z-SNAPSHOT-jar-with-dependencies.jar

Javadoc documentation is generated automatically and can then be found in the target/apidocs subdirectory.

Finally, unit test can be run via:

    mvn test

-------------
Library usage
-------------

Hadoop-BAM provides the standard set of Hadoop file format classes for the file
formats it supports: a FileInputFormat and one or more RecordReaders for input,
and a FileOutputFormat and one or more RecordWriters for output.

Note that Hadoop-BAM is based around the newer Hadoop API introduced in the
0.20 Hadoop releases instead of the older, deprecated API.

For examples of how to link to Hadoop-BAM in your own Maven project
see the examples/ folder. There are example for reading and writing
BAM as well as VCF files.

For more information see the Javadoc as well as the command line plugins'
source code (in src/main/java/org/seqdoop/hadoop_bam/cli/plugins/*.java).
In particular, for MapReduce usage, recommended examples are
src/main/java/org/seqdoop/hadoop_bam/cli/plugins/FixMate.java and
src/main/java/org/seqdoop/hadoop_bam/cli/plugins/VCFSort.java.

When using Hadoop-BAM as a library in your program, remember to have
hadoop-bam-X.Y.Z.jar as well as the Picard .jars (including the Commons
JEXL .jar) in your CLASSPATH and HADOOP_CLASSPATH; alternatively, use the
*-jar-with-dependencies.jar which contains already all dependencies.

Linking against Hadoop-BAM
--------------------------

If your Maven project relies on Hadoop-BAM the easiest way to link against
it is by relying on the OSS Sonatype repository:

~~~~~~~
<project>
...
    <dependencies>
        <dependency>
            <groupId>org.seqdoop</groupId>
            <artifactId>hadoop-bam</artifactId>
            <version>7.3.1</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
~~~~~~~

------------------
Command-line usage
------------------

Hadoop-BAM can be used as a command-line tool, with functionality in the form
of plugins that provide commands to which hadoop-bam-X.Y.Z.jar is a frontend.
Hadoop-BAM provides some commands of its own, but any others found in the Java
class path will be used as well.

Running under Hadoop
--------------------

To use Hadoop-BAM under Hadoop, the easiest method is to use the
jar that comes packaged with all dependencies via

    hadoop jar hadoop-bam-X.Y.Z-jar-with-dependencies.jar

Alternatively, you can use the "-libjars" command line argument when
running Hadoop-BAM to provide different versions of dependencies as follows:

    hadoop jar hadoop-bam-X.Y.Z.jar \
      -libjars htsjdk-1.118.jar,commons-jexl-2.1.1.jar

Finally, all jar files can also be added to HADOOP_CLASSPATH in the Hadoop
configuration's hadoop-env.sh.

The command used should print a brief help message listing the Hadoop-BAM
commands available. To run a command, give it as the first command-line
argument. For example, the provided SAM/BAM sorting command, "sort":

    hadoop jar hadoop-bam-X.Y.Z-jar-with-dependencies.jar sort

This will give a help message specific to that command.

File paths under Hadoop
-----------------------

When running under Hadoop, keep in mind that file paths refer to the
distributed file system, HDFS. To explicitly access a local file, instead of
using the plain path such as "/foo/bar", you must use a file: URI, such as
"file:/foo/bar". Note that paths in file: URIs must be absolute.

Output of MapReduce-using commands
----------------------------------

An example of a MapReduce-using command is "sort". Like all such commands
should, it takes a working directory argument in which to place its output in
parts. Each part is the output of one reduce task. By default, these parts are
not complete and usable files! They are /not/ BAM or SAM files, they are only
parts of BAM or SAM files containing output records, but lacking headers and
footers.

For convenience, the provided MapReduce-using commands support a "-o" parameter
to output single complete files instead of the individual parts.

For concatenating the outputs of tools that wish to output complete SAM and BAM
files from each reducer, the "cat" command is provided.

Note that some commands, such as the provided "view" and "index" commands, do
not use MapReduce: they are merely useful to operate directly on files stored
in HDFS.

Running without Hadoop
----------------------

Hadoop-BAM can be run directly, outside Hadoop, as long as it and the Picard
and Hadoop .jar files as well as the Apache Commons CLI .jar provided by Hadoop
("lib/commons-cli-1.2.jar" for version 1.1.2) are in the Java class path.
Alternatively use the bundled jar (hadoop-bam-jar-with-dependencies-X.Y.Z.jar). In
addition, depending on the Hadoop version, there may be more dependencies from
the Hadoop lib/ directory. A command such as the following:

    java org.seqdoop.hadoop_bam.cli.Frontend

Is equivalent to the "hadoop jar hadoop-bam-X.Y.Z.jar" command used earlier. This has
limited application, but it can be used e.g. for testing purposes.

Note that the "-libjars" way of passing the paths to the Picard .jars will not
work when running Hadoop-BAM like this.

------------------
Summarizer plugins
------------------

This part explains some behaviour of the summarizing plugins, available in the
command line interface as "hadoop jar hadoop-bam-X.Y.Z.jar summarize" and "hadoop jar
hadoop-bam-X.Y.Z.jar summarysort". Unless you are a Chipster user, this section is
unlikely to be relevant to you, and even then, this is not likely to be
something you are interested in.

Summarization is typically best done with the "hadoop jar hadoop-bam-X.Y.Z.jar
summarize --sort -o output-directory" command. Then there is no need to worry
about concatenating nor sorting the output, as both are done automatically in
this one command. But if you do not pass the "--sort" option, do remember that
Chipster needs the outputs sorted before it can make use of them. For this, you
need to run a separate "hadoop jar hadoop-bam-X.Y.Z.jar summarysort" command for each
summary file output by "summarize".

Output format
-------------

The summarizer's output format is tabix-compatible. It is composed of rows of
tab-separated data:

    <reference sequence ID> <left coordinate> <right coordinate> <count>

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
