A BAM file summarizer plugin for hadoop-bam, with output designed for
Chipster's genome browser.

Installation
------------

Making sure that hadoop-bam.jar and its dependencies are in your Java class
path, run:

	ant jar

Giving 'summarize.jar'. This should be placed in your class path (in
particular, for Hadoop, in the HADOOP_CLASSPATH in the hadoop-env.sh
configuration file) so that hadoop-bam.jar can find the plugin.

Usage
-----

The summarizer is to be run under hadoop-bam as the "summarize" command, with a
command line such as:

	hadoop jar hadoop-bam.jar summarize

Without any further arguments, a brief help message will be output.

Before Chipster can use the created summaries, they have to be sorted. The
"summarize" command itself supports this with the "--sort" parameter, but it
can also be done separately via the "summarysort" command:

	hadoop jar hadoop-bam.jar summarysort

Note that each summary file needs to be sorted individually. "summarize --sort"
does this for you.

Output format
-------------

The output format is tabix-compatible. It is composed of rows of tab-separated
data:

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
