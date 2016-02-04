package org.seqdoop.hadoop_bam;

import htsjdk.samtools.SAMFileHeader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;

/** A convenience class that you can use as a RecordWriter for CRAM files.
 *
 * <p>The write function ignores the key, just outputting the SAMRecord.</p>
 */
public class KeyIgnoringCRAMRecordWriter<K> extends CRAMRecordWriter<K> {
    public KeyIgnoringCRAMRecordWriter(
            Path output, Path input, boolean writeHeader, TaskAttemptContext ctx)
            throws IOException
    {
        super(output, input, writeHeader, ctx);
    }

    public KeyIgnoringCRAMRecordWriter(
            Path output, SAMFileHeader header, boolean writeHeader,
            TaskAttemptContext ctx)
            throws IOException
    {
        super(output, header, writeHeader, ctx);
    }

    @Override public void write(K ignored, SAMRecordWritable rec) {
        writeAlignment(rec.get());
    }
}
