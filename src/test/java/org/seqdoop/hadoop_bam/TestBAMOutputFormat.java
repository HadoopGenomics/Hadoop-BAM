package org.seqdoop.hadoop_bam;

import htsjdk.samtools.*;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import org.seqdoop.hadoop_bam.util.SAMOutputPreparer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestBAMOutputFormat {
    private String testBAMFileName;

    private int expectedRecordCount;
    private SAMFileHeader samFileHeader;

    private TaskAttemptContext taskAttemptContext;
    static private Configuration conf;

    // BAM output class that writes a header before records
    static class BAMTestWithHeaderOutputFormat
            extends KeyIgnoringBAMOutputFormat<NullWritable> {
        public final static String READ_HEADER_FROM_FILE = "TestBAM.header";

        @Override
        public RecordWriter<NullWritable, SAMRecordWritable> getRecordWriter(
                TaskAttemptContext ctx,
                Path outputPath) throws IOException {
            readSAMHeaderFrom(new Path(conf.get(READ_HEADER_FROM_FILE)), conf);
            setWriteHeader(true);
            return super.getRecordWriter(ctx, outputPath);
        }
    }

    // BAM output class that doesn't write a header before records
    static class BAMTestNoHeaderOutputFormat
            extends KeyIgnoringBAMOutputFormat<NullWritable> {
        public final static String READ_HEADER_FROM_FILE = "TestBAM.header";

        @Override
        public RecordWriter<NullWritable, SAMRecordWritable> getRecordWriter(
                TaskAttemptContext ctx,
                Path outputPath) throws IOException {
            // the writers require a header in order to create a codec, even if
            // the header isn't being written out
            readSAMHeaderFrom(new Path(conf.get(READ_HEADER_FROM_FILE)), conf);
            setWriteHeader(false);
            return super.getRecordWriter(ctx, outputPath);
        }
    }

    @Before
    public void setup() throws Exception {
        conf = new Configuration();

        testBAMFileName = ClassLoader.getSystemClassLoader()
                                .getResource("test.bam").getFile();

        conf.set("mapred.input.dir", "file://" + testBAMFileName);

        // fetch the SAMFile header from the original input to get the expected count
        expectedRecordCount = getBAMRecordCount(testBAMFileName);
        samFileHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(testBAMFileName), conf);

        taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
    }

    @Test
    public void testBAMRecordWriterNoHeader() throws Exception {
        final File outFile = File.createTempFile("testBAMWriter", ".bam");
        outFile.deleteOnExit();
        final Path outPath = new Path(outFile.toURI());

        final BAMTestNoHeaderOutputFormat bamOut = new BAMTestNoHeaderOutputFormat();
        conf.set(BAMTestNoHeaderOutputFormat.READ_HEADER_FROM_FILE, testBAMFileName);
        bamOut.setWriteHeader(false);

        RecordWriter<NullWritable, SAMRecordWritable> rw =
                bamOut.getRecordWriter(taskAttemptContext, outPath);

        final SamReader samReader = SamReaderFactory.makeDefault()
                .open(new File(testBAMFileName));

        for (final SAMRecord r : samReader) {
            final SAMRecordWritable samRW = new SAMRecordWritable();
            samRW.set(r);
            rw.write(null, samRW);
        }
        samReader.close();
        rw.close(taskAttemptContext);

        // now verify the output
        final int actualCount = verifyBAMBlocks(
                new File(outFile.getAbsolutePath()),
                samFileHeader,
                true);

        assertEquals(expectedRecordCount, actualCount);
    }

    @Test
    public void testBAMRecordWriterWithHeader() throws Exception {
        final File outFile = File.createTempFile("testBAMWriter", ".bam");
        outFile.deleteOnExit();
        final Path outPath = new Path(outFile.toURI());

        final BAMTestWithHeaderOutputFormat bamOut = new BAMTestWithHeaderOutputFormat();
        conf.set(BAMTestWithHeaderOutputFormat.READ_HEADER_FROM_FILE, testBAMFileName);
        bamOut.setWriteHeader(false);

        RecordWriter<NullWritable, SAMRecordWritable> rw =
                bamOut.getRecordWriter(taskAttemptContext, outPath);

        final SamReader samReader = SamReaderFactory.makeDefault()
                .open(new File(testBAMFileName));

        for (final SAMRecord r : samReader) {
            final SAMRecordWritable samRW = new SAMRecordWritable();
            samRW.set(r);
            rw.write(null, samRW);
        }
        samReader.close();
        rw.close(taskAttemptContext);

        // now verify the output
        final int actualCount = verifyBAMBlocks(
                new File(outFile.getAbsolutePath()),
                samFileHeader,
                false);

        assertEquals(expectedRecordCount, actualCount);
    }

    @Test
    public void testBAMOutput() throws Exception {
        final Path outputPath = doMapReduce(testBAMFileName);
        final File blockStreamFile =
                new File(new File(outputPath.toUri()), "part-m-00000");
        final int actualCount = verifyBAMBlocks(
                blockStreamFile, samFileHeader, true);
        assertEquals(expectedRecordCount, actualCount);
    }

    @Test
    public void testBAMRoundTrip() throws Exception {
        // run a m/r job to write out a bam file
        Path outputPath = doMapReduce(testBAMFileName);

        // assemble the output, and write to a temp file
        File blockStreamFile =
                new File(new File(outputPath.toUri()), "part-m-00000");
        final ByteArrayInputStream bamStream = mergeBAMBlockStream(
                blockStreamFile,
                samFileHeader,
                true);
        final File outFile = File.createTempFile("testBAMWriter", ".bam");
        outFile.deleteOnExit();
        Files.copy(bamStream, outFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        // now use the assembled output as m/r input
        outputPath = doMapReduce(outFile.getAbsolutePath());

        // verify the final output
        blockStreamFile = new File(new File(outputPath.toUri()), "part-m-00000");
        final int actualCount = verifyBAMBlocks(
                blockStreamFile,
                samFileHeader,
                true);
        assertEquals(expectedRecordCount, actualCount);
    }

    private int getBAMRecordCount(final String bamFileName) throws IOException {
        final SamReader bamReader = SamReaderFactory.makeDefault()
                                        .open(SamInputResource.of(bamFileName));
        final Iterator<SAMRecord> it = bamReader.iterator();
        int recCount = 0;
        while (it.hasNext()) {
            it.next();
            recCount++;
        }
        bamReader.close();
        return recCount;
    }

    private Path doMapReduce(final String inputFile) throws Exception {
        final FileSystem fileSystem = FileSystem.get(conf);
        final Path inputPath = new Path(inputFile);
        final Path outputPath = fileSystem.makeQualified(new Path("target/out"));
        fileSystem.delete(outputPath, true);

        final Job job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, inputPath);

        conf.set(BAMTestNoHeaderOutputFormat.READ_HEADER_FROM_FILE, inputFile);
        job.setInputFormatClass(BAMInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(SAMRecordWritable.class);

        job.setOutputFormatClass(BAMTestNoHeaderOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(SAMRecordWritable.class);

        job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, outputPath);

        final boolean success = job.waitForCompletion(true);
        assertTrue(success);

        return outputPath;
    }

    private int verifyBAMBlocks(
            final File blockStreamFile,
            final SAMFileHeader header,
            final boolean writeHeader) throws IOException
    {
        // assemble a proper BAM file from the block stream shard(s) in
        // order to verify the contents
        final ByteArrayInputStream mergedStream = mergeBAMBlockStream (
                blockStreamFile,
                header,
                writeHeader
        );

        // now we can verify that we can read everything back in
        final SamReader resultBAMReader = SamReaderFactory.makeDefault()
                                        .open(SamInputResource.of(mergedStream));
        final Iterator<SAMRecord> it = resultBAMReader.iterator();
        int actualCount = 0;
        while (it.hasNext()) {
            it.next();
            actualCount++;
        }
        return actualCount;
    }

    private ByteArrayInputStream mergeBAMBlockStream(
            final File blockStreamFile,
            final SAMFileHeader header,
            final boolean writeHeader) throws IOException
    {
        // assemble a proper BAM file from the block stream shard(s) in
        // order to verify the contents
        final ByteArrayOutputStream bamOutputStream = new ByteArrayOutputStream();

        // write out the bam file header
        if (writeHeader) {
            new SAMOutputPreparer().prepareForRecords(
                    bamOutputStream,
                    SAMFormat.BAM,
                    header);
        }

        // copy the contents of the block shard(s) written out by the M/R job
        final ByteArrayOutputStream blockOutputStream = new ByteArrayOutputStream();
        Files.copy(blockStreamFile.toPath(), blockOutputStream);
        blockOutputStream.writeTo(bamOutputStream);

        // add the BGZF terminator
        bamOutputStream.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
        bamOutputStream.close();

        return new ByteArrayInputStream(bamOutputStream.toByteArray());
    }

}
