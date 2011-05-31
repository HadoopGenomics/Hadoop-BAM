hadoop-bam: a library for manipulation of BAM (Binary Alignment/Map) and
BGZF-compressed files using the Hadoop MapReduce framework.

Includes programs for indexing both BAM and BGZF files, allowing Hadoop to
split them, as well as an example program for sorting BAM files.

Dependencies
------------

Hadoop 0.20.2. Tested 0.21: this version of hadoop-bam is incompatible with it.

Picard 1.27. Later versions have not been tested: use at your own risk.

Availability:
	Hadoop - http://hadoop.apache.org/
	Picard - http://picard.sourceforge.net/

Installation
------------

The easiest way to build hadoop-bam is to use Apache Ant (version 1.6 or
greater) with the following command:

	ant jar

This will create the 'hadoop-bam.jar' file. For Javadoc documentation, run:

	ant javadoc

Documentation can then be found in the 'doc' subdirectory.

General usage
-------------

In order to use all the functionality of hadoop-bam, you need to have Picard's
'sam-1.27.jar' (assuming version 1.27) and Hadoop's 'hadoop-0.20.2-core.jar' in
the CLASSPATH environment variable.

See the Javadoc as well as the BAM sorter's source code
(src/fi/tkk/ics/hadoop/bam/util/hadoop/BAMSort.java) for library usage
information.

Example programs
----------------

Two utilities that are meant for usage from within Hadoop are included. To run,
use commands such as:

	hadoop jar hadoop-bam.jar fi.tkk.ics.hadoop.bam.util.hadoop.BAMReader
	hadoop jar hadoop-bam.jar fi.tkk.ics.hadoop.bam.util.hadoop.BAMSort

Run them without passing any further arguments for a brief help message.

BAMReader is a simple proof-of-concept-style tool for reading BAM files from
HDFS directly, without having to transfer them out to a local disk.

BAMSort is a MapReduce-using application that sorts a BAM file. In distributed
usage, in order to compose the final result file from multiple parts, you may
simply concatenate the files together, along with a header retrieved using
GetSortedBAMHeader (see "utilities" section below) at the beginning, and the
BGZF terminator block, provided as bgzf-terminator.bin, at the end. Using
'hadoop fs -getmerge' when retrieving the files out of HDFS is likely the most
efficient way of doing this.

Utilities
---------

Five non-distributed utility programs are included. They can be invoked as
follows, with hadoop-bam.jar in the CLASSPATH environment variable:

       java fi.tkk.ics.hadoop.bam.util.GetSortedBAMHeader
       java fi.tkk.ics.hadoop.bam.SplittingBAMIndexer
       java fi.tkk.ics.hadoop.bam.SplittingBAMIndex
       java fi.tkk.ics.hadoop.bam.util.BGZFBlockIndexer
       java fi.tkk.ics.hadoop.bam.util.BGZFBlockIndex

Again, run them without arguments for a brief help message.

GetSortedBAMHeader reads the SAM header from the input BAM file, sets the 'sort
order' metadata to 'coordinate', and outputs the result to the output file.
This is meant to be used with the BAM sorter.

The remaining four, concerning indexing, are all deprecated. With the heuristic
splitting introduced in hadoop-bam 2.0, there shouldn't be any reason to use
them. Nevertheless, they are still included and explained below.

Running SplittingBAMIndexer on a BAM file creates a .splitting-bai file which
is used by BAMInputFormat to determine how to split up work between map tasks.
Note that indexing can be a time-consuming process if the BAM file is large.

The granularity argument does not matter much: in practice, what it specifies
is the maximum amount of alignments (respectively gzip blocks, for
BGZFBlockIndexer) that may be "shunted" from one Hadoop mapper to another. If
Hadoop places a split point between two offsets in the index file, it is
rounded to one of them. A reasonable value for SplittingBAMIndexer granularity
is, for example, 1024: this results in a 3-megabyte index for a 50-gigabyte BAM
file.

BGZFBlockIndexer is similar, but for working with BGZF-compressed files without
caring about the type of data within. If you've only got BAM files, you
probably don't want to deal with them at this level.
