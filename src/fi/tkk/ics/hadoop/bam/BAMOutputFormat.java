// File created: 2010-08-11 12:17:33

package fi.tkk.ics.hadoop.bam;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Currently this only locks down the value type of the {@link
 * org.apache.hadoop.mapreduce.OutputFormat}: contains no functionality.
 */
public abstract class BAMOutputFormat<K>
	extends FileOutputFormat<K,SAMRecordWritable>
{}
