This directory contains examples for how to use Hadoop-BAM as a
library. To build the examples do:

$ mvn clean package

To run the examples:

$ hadoop jar target/*-jar-with-dependencies.jar \
   org.seqdoop.hadoop_bam.examples.TestBAM <input.bam> <output_directory>

and:

$ hadoop jar target/*-jar-with-dependencies.jar \
   org.seqdoop.hadoop_bam.examples.TestVCF <input.vcf> <output_directory>
