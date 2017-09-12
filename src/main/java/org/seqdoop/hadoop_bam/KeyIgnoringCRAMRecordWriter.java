package org.seqdoop.hadoop_bam;

import java.io.IOException;
import htsjdk.samtools.SAMFileHeader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A convenience class that you can use as a RecordWriter for CRAM files.
 * <p>
 * <p>The write function ignores the key, just outputting the SAMRecord.</p>
 */
public class KeyIgnoringCRAMRecordWriter<K> extends CRAMRecordWriter<K> {
    public KeyIgnoringCRAMRecordWriter(final Path output,
                                       final Path input,
                                       final boolean writeHeader,
                                       final TaskAttemptContext ctx)
            throws IOException {
        super(output, input, writeHeader, ctx);
    }

    public KeyIgnoringCRAMRecordWriter(final Path output,
                                       final SAMFileHeader header,
                                       final boolean writeHeader,
                                       final TaskAttemptContext ctx)
            throws IOException {
        super(output, header, writeHeader, ctx);
    }

    @Override
    public void write(K ignored, SAMRecordWritable rec) {
        writeAlignment(rec.get());
    }
}
