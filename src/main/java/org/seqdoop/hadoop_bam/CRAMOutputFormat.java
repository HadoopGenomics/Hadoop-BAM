package org.seqdoop.hadoop_bam;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Currently this only locks down the value type of the {@link
 * org.apache.hadoop.mapreduce.OutputFormat}: contains no functionality.
 */
public abstract class CRAMOutputFormat<K>
        extends FileOutputFormat<K,SAMRecordWritable>
{}
