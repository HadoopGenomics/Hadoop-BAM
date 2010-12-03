A BAM file summarizer, for Chipster's genome browser.

Installation
------------

Making sure that hadoop-bam.jar and its dependencies are in your CLASSPATH,
run:

	./build.sh

Giving 'summarizer.jar'.

Usage
-----

Run under Hadoop with a command such as:

	hadoop jar summarizer.jar Summarize

Without any further arguments, a brief help message will be output.

Note that each input BAM file needs to have a corresponding .splitting-bai
file: see the documentation of the hadoop-bam tools for how to generate it.

Output format
-------------

The output format is tabix-compatible. It is composed of rows of tab-separated
data:

	<reference sequence ID>	<left coordinate> <right coordinate> <count>

The coordinate columns are 1-based.

The 'count' field represents the number of alignments that have been summarized
into that single range. Note that it may not exactly match any of the 'level'
arguments passed to Summarize, due to Hadoop splitting the file at a boundary
which is not an even multiple of the requested level.

Note that the output files are BGZF-compressed, but do not include the empty
terminating block which would make them valid BGZF files. This is to avoid
having to remove it from the end of each output file in distributed usage: it's
much simpler to concatenate the bgzf-terminator.bin file provided with
hadoop-bam to the end of the output.
