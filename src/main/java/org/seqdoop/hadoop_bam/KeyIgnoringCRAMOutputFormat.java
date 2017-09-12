package org.seqdoop.hadoop_bam;

import java.io.IOException;
import java.io.InputStream;
import htsjdk.samtools.SAMFileHeader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

/**
 * Writes only the BAM records, not the key.
 * <p>
 * <p>A {@link SAMFileHeader} must be provided via {@link #setSAMHeader} or
 * {@link #readSAMHeaderFrom} before {@link #getRecordWriter} is called.</p>
 * <p>
 * <p>By default, writes the SAM header to the output file(s). This
 * can be disabled, because in distributed usage one often ends up with (and,
 * for decent performance, wants to end up with) the output split into multiple
 * parts, which are easier to concatenate if the header is not present in each
 * file.</p>
 */
public class KeyIgnoringCRAMOutputFormat<K> extends CRAMOutputFormat<K> {
    protected SAMFileHeader header;
    private boolean writeHeader = true;

    public KeyIgnoringCRAMOutputFormat() {
    }

    /**
     * Whether the header will be written or not.
     */
    public boolean getWriteHeader() {
        return writeHeader;
    }

    /**
     * Set whether the header will be written or not.
     */
    public void setWriteHeader(final boolean b) {
        writeHeader = b;
    }

    public SAMFileHeader getSAMHeader() {
        return header;
    }

    public void setSAMHeader(final SAMFileHeader header) {
        this.header = header;
    }

    public void readSAMHeaderFrom(final Path path, final Configuration conf)
            throws IOException {
        this.header = SAMHeaderReader.readSAMHeaderFrom(path, conf);
    }

    public void readSAMHeaderFrom(final InputStream in, final Configuration conf) {
        this.header = SAMHeaderReader.readSAMHeaderFrom(in, conf);
    }

    /**
     * <code>setSAMHeader</code> or <code>readSAMHeaderFrom</code> must have
     * been called first.
     */
    @Override
    public RecordWriter<K, SAMRecordWritable> getRecordWriter(
            final TaskAttemptContext ctx)
            throws IOException {
        return getRecordWriter(ctx, getDefaultWorkFile(ctx, ""));
    }

    // Allows wrappers to provide their own work file.
    public RecordWriter<K, SAMRecordWritable> getRecordWriter(
            final TaskAttemptContext ctx, final Path out)
            throws IOException {
        if (this.header == null) {
            throw new IOException(
                    "Can't create a RecordWriter without the SAM header");
        }

        return new KeyIgnoringCRAMRecordWriter<K>(out, header, writeHeader, ctx);
    }
}
