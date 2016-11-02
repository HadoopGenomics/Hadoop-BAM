package org.seqdoop.hadoop_bam;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;

import htsjdk.samtools.CRAMContainerStreamWriter;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.samtools.util.StringLineReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.seqdoop.hadoop_bam.util.NIOFileUtil;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

/** A base {@link RecordWriter} for CRAM records.
 *
 * <p>Handles the output stream, writing the header if requested, and provides
 * the {@link #writeAlignment} function for subclasses.</p>
 * <p>Note that each file created by this class consists of a fragment of a
 * complete CRAM file containing only one or more CRAM containers that do not
 * include a CRAM file header, a SAMFileHeader, or a CRAM EOF container.</p>
 */
public abstract class CRAMRecordWriter<K>
        extends RecordWriter<K,SAMRecordWritable>
{
    // generic ID passed to CRAM code for internal error reporting
    private static final String HADOOP_BAM_PART_ID= "Hadoop-BAM-Part";
    private OutputStream   origOutput;
    private CRAMContainerStreamWriter cramContainerStream = null;
    private ReferenceSource refSource = null;
    private boolean writeHeader = true;

    /** A SAMFileHeader is read from the input Path. */
    public CRAMRecordWriter(
            final Path output,
            final Path input,
            final boolean writeHeader,
            final TaskAttemptContext ctx) throws IOException
    {
        init(
                output,
                SAMHeaderReader.readSAMHeaderFrom(input, ctx.getConfiguration()),
                writeHeader, ctx);
    }

    public CRAMRecordWriter(
            final Path output, final SAMFileHeader header, final boolean writeHeader,
            final TaskAttemptContext ctx)
            throws IOException
    {
        init(
                output.getFileSystem(ctx.getConfiguration()).create(output),
                header, writeHeader, ctx);
    }

    // Working around not being able to call a constructor other than as the
    // first statement...
    private void init(
            final Path output, final SAMFileHeader header, final boolean writeHeader,
            final TaskAttemptContext ctx)
            throws IOException
    {
        init(
                output.getFileSystem(ctx.getConfiguration()).create(output),
                header, writeHeader, ctx);
    }

    private void init(
            final OutputStream output, final SAMFileHeader header, final boolean writeHeader,
            final TaskAttemptContext ctx)
            throws IOException
    {
        origOutput = output;
        this.writeHeader = writeHeader;

        final String referenceURI =
                ctx.getConfiguration().get(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY);
        refSource = new ReferenceSource(referenceURI == null ? null :
            NIOFileUtil.asPath(referenceURI));

        // A SAMFileHeader must be supplied at CRAMContainerStreamWriter creation time; if
        // we don't have one then delay creation until we do
        if (header != null) {
            cramContainerStream = new CRAMContainerStreamWriter(
                    origOutput, null, refSource, header, HADOOP_BAM_PART_ID);
            if (writeHeader) {
                this.writeHeader(header);
            }
        }
    }

    @Override public void close(TaskAttemptContext ctx) throws IOException {
        cramContainerStream.finish(false); // Close, but suppress CRAM EOF container
        origOutput.close(); // And close the original output.
    }

    protected void writeAlignment(final SAMRecord rec) {
        if (null == cramContainerStream) {
            final SAMFileHeader header = rec.getHeader();
            if (header == null) {
                throw new RuntimeException("Cannot write record to CRAM: null header in SAM record");
            }
            if (writeHeader) {
                this.writeHeader(header);
            }
            cramContainerStream = new CRAMContainerStreamWriter(
                    origOutput, null, refSource, header, HADOOP_BAM_PART_ID);
        }
        cramContainerStream.writeAlignment(rec);
    }

    private void writeHeader(final SAMFileHeader header) {
        cramContainerStream.writeHeader(header);
    }
}
