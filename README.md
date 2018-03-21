
  Hadoop-BAM
==============

Hadoop-BAM: a library for the manipulation of files in common bioinformatics
formats using the Hadoop MapReduce framework.

Build status
------------
[![Build Status](https://travis-ci.org/HadoopGenomics/Hadoop-BAM.svg?branch=master)](https://travis-ci.org/HadoopGenomics/Hadoop-BAM)
[![Coverage Status](https://coveralls.io/repos/github/HadoopGenomics/Hadoop-BAM/badge.svg?branch=master)](https://coveralls.io/github/HadoopGenomics/Hadoop-BAM?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.seqdoop/hadoop-bam/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.seqdoop/hadoop-bam)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

The [file formats](http://samtools.github.io/hts-specs/) currently supported are:

   - BAM (Binary Alignment/Map)
   - SAM (Sequence Alignment/Map)
   - CRAM
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
with experience in using Hadoop. SeqPig is a more
versatile and higher-level interface to the file formats supported by
Hadoop-BAM. In addition, [ADAM](http://bdgenomics.org/) and
[GATK version 4](https://github.com/broadinstitute/gatk) both use Hadoop-BAM and offer 
high-level command-line bioinformatics tools that run on Spark clusters. 

For examples of how to use Hadoop-BAM as a library to read data files
in Hadoop see the `examples/` directory.

------------
Dependencies
------------

Hadoop. Any recent version should work. Hadoop is a "provided" dependency in the Maven sense,
which means that you must have Hadoop installed, or, if using Hadoop-BAM as a library, have the
Hadoop Maven dependencies in your POM.

[HTSJDK](https://github.com/samtools/htsjdk). Hadoop-BAM uses a particular version, but later ones can usually be substituted.

Note that starting from version 7.4.0 Hadoop-BAM requires Java 8.

------------
Installation
------------

A precompiled "hadoop-bam-X.Y.Z.jar" is available that you can use with most recent versions of
Hadoop.  Otherwise, you'll have to build Hadoop-BAM yourself by using Maven and following the
instructions below.

### Build

Build Hadoop-BAM with the following command:

    mvn clean package -DskipTests
   
It will create two files:

    target/hadoop-bam-X.Y.Z-SNAPSHOT.jar
    target/hadoop-bam-X.Y.Z-SNAPSHOT-jar-with-dependencies.jar

The former contains only Hadoop-BAM whereas the latter one also contains all
dependencies and can be run directly via

    hadoop jar target/hadoop-bam-X.Y.Z-SNAPSHOT-jar-with-dependencies.jar

Javadoc documentation is generated automatically and can then be found in the
`target/apidocs` subdirectory.

Finally, unit tests can be run via:

    mvn test

-------------
Library usage
-------------

Hadoop-BAM provides the standard set of Hadoop file format classes for the file
formats it supports: a FileInputFormat and one or more RecordReaders for input,
and a FileOutputFormat and one or more RecordWriters for output. These are summarized 
in the table below.

|File Format|InputFormat|OutputFormat|
|-----------|-----------|------------|
|BAM|`BAMInputFormat`|`KeyIgnoringBAMOutputFormat`|
|SAM|`SAMInputFormat`|`KeyIgnoringAnySAMOutputFormat`|
|CRAM|`CRAMInputFormat`|`KeyIgnoringCRAMOutputFormat`|
|BAM, SAM, or CRAM|`AnySAMInputFormat`|`KeyIgnoringAnySAMOutputFormat`|
|FASTQ|`FastqInputFormat`|`FastqOutputFormat`|
|FASTA|`FastaInputFormat`|N/A|
|QSEQ|`QseqInputFormat`|`QseqOutputFormat`|
|VCF or BCF|`VCFInputFormat`|`KeyIgnoringVCFOutputFormat`|

`AnySAMInputFormat` detects the format (BAM, SAM, or CRAM) by file extension, then by
looking at the first few bytes of the file (if file extension detection is disabled or
 is inconclusive). `VCFInputFormat` works in a similar way for VCF and BCF files.
 
The output formats all discard the key and only use the value field when writing the 
output file. Some of the output formats indicate this explictly through the 
`KeyIgnoring` prefix in their name, but `FastqOutputFormat` and `QseqOutputFormat` 
actually ignore the key too.

The abstract base classes `BAMOutputFormat`, `CRAMOutputFormat`, `AnySAMOutputFormat`, 
and `VCFOutputFormat` cannot be used directly, but can be subclassed in order to add 
custom logic (to provide `ValueIgnoring` versions, for example). 

When using `KeyIgnoringAnySAMOutputFormat`, the format of the files written (BAM, SAM, or CRAM) 
must be specified by setting the property `hadoopbam.anysam.output-format`. Similarly,
set the property `hadoopbam.vcf.output-format` in order to specify which file format
`KeyIgnoringVCFOutputFormat` will use (VCF or BCF).

The properties that can be set on input and output formats are summarized in the table 
below:

|Format|Property|Default|Description|
|------|--------|-------|-----------|
|`AnySAMInputFormat`|`hadoopbam.anysam.trust-exts`|`true`|Whether to detect the file format (BAM, SAM, or CRAM) by file extension. If `false`, use the file contents to detect the format.|
|`KeyIgnoringAnySAMOutputFormat`|`hadoopbam.anysam.output-format`| |(Required.) The file format to use when writing BAM, SAM, or CRAM files. Should be one of `BAM`, `SAM`, or `CRAM`.|
| |`hadoopbam.anysam.write-header`|`true`|Whether to write the SAM header in each output file part. If `true`, call `setSAMHeader()` or `readSAMHeaderFrom()` to set the desired header.|
|`BAMInputFormat`|`hadoopbam.bam.bounded-traversal`|`false`|If `true`, only include reads that match the intervals specified in `hadoopbam.bam.intervals`, and unplaced unmapped reads, if `hadoopbam.bam.traverse-unplaced-unmapped` is set to `true`.|
| |`hadoopbam.bam.intervals`| |Only include reads that match the specified intervals. BAM files must be indexed in this case. Intervals are comma-separated and follow the same syntax as the `-L` option in SAMtools. E.g. `chr1:1-20000,chr2:12000-20000`. When using this option `hadoopbam.bam.bounded-traversal` should be set to `true`.|
| |`hadoopbam.bam.traverse-unplaced-unmapped`|`false`|If `true`, include unplaced unmapped reads (that is, unmapped reads with no position) When using this option `hadoopbam.bam.bounded-traversal` should be set to `true`.|
| |`hadoopbam.bam.use-intel-inflater`|`false`|If `true`, use the Intel inflater for decompressing DEFLATE compressed streams. When using this option the [GKL library](https://github.com/Intel-HLS/GKL) must be provided on the classpath.|
|`KeyIgnoringBAMOutputFormat`|`hadoopbam.bam.write-splitting-bai`|`false`|If `true`, write _.splitting-bai_ files for every BAM file.|
| |`hadoopbam.bam.use-intel-deflater`|`false`|If `true`, use the Intel deflater for compressing DEFLATE compressed streams. When using this option the [GKL library](https://github.com/Intel-HLS/GKL) must be provided on the classpath.|
|`CRAMInputFormat`|`hadoopbam.cram.reference-source-path`| |(Required.) The path to the reference. May be an `hdfs://` path.|
|`FastqInputFormat`|`hbam.fastq-input.base-quality-encoding`|`sanger`|The encoding used for base qualities. One of `sanger` or `illumina`.|
| |`hbam.fastq-input.filter-failed-qc`|`false`|If `true`, filter out reads that didn't pass quality checks.|
|`QseqInputFormat`|`hbam.qseq-input.base-quality-encoding`|`illumina`|The encoding used for base qualities. One of `sanger` or `illumina`.|
| |`hbam.qseq-input.filter-failed-qc`|`false`|If `true`, filter out reads that didn't pass quality checks.|
|`VCFInputFormat`|`hadoopbam.vcf.trust-exts`|`false`|Whether to detect the file format (VCF or BCF) by file extension. If `false`, use the file contents to detect the format.|
|`KeyIgnoringVCFOutputFormat`|`hadoopbam.vcf.output-format`| |(Required.) The file format to use when writing VCF or BCF files. Should be one of `VCF` or `BCF`.|
| |`hadoopbam.vcf.write-header`|`true`|Whether to write the VCF header in each output file part. If `true`, call `setHeader()` or `readHeaderFrom()` to set the desired header.|

Note that Hadoop-BAM is based around the newer Hadoop API introduced in the
0.20 Hadoop releases instead of the older, deprecated API.

For examples of how to link to Hadoop-BAM in your own Maven project
see the `examples/` folder. There are examples for reading and writing
BAM as well as VCF files.

When using Hadoop-BAM as a library in your program, remember to have
hadoop-bam-X.Y.Z.jar as well as the HTSJDK .jars (including the Commons
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
            <version>7.9.2</version>
        </dependency>
        ...
    </dependencies>
    ...
</project>
~~~~~~~

------------------
Running under Hadoop
--------------------

To use Hadoop-BAM under Hadoop, the easiest method is to use the
jar that comes packaged with all dependencies via

    hadoop jar hadoop-bam-X.Y.Z-jar-with-dependencies.jar

Alternatively, you can use the "-libjars" command line argument when
running Hadoop-BAM to provide different versions of dependencies as follows:

    hadoop jar hadoop-bam-X.Y.Z.jar \
      -libjars htsjdk-2.3.0.jar,commons-jexl-2.1.1.jar

Finally, all jar files can also be added to HADOOP_CLASSPATH in the Hadoop
configuration's hadoop-env.sh.

File paths under Hadoop
-----------------------

When running under Hadoop, keep in mind that file paths refer to the
distributed file system, HDFS. To explicitly access a local file, instead of
using the plain path such as "/foo/bar", you must use a file: URI, such as
"file:/foo/bar". Note that paths in file: URIs must be absolute.
