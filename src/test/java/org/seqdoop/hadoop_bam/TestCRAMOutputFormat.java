package org.seqdoop.hadoop_bam;

import htsjdk.samtools.*;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;
import org.seqdoop.hadoop_bam.util.SAMFileMerger;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

import java.io.*;
import java.nio.file.Paths;
import java.util.Iterator;
import org.seqdoop.hadoop_bam.util.SAMOutputPreparer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestCRAMOutputFormat {
    private String testCRAMFileName;
    private String testReferenceFileName;
    private ReferenceSource testReferenceSource;

    private int expectedRecordCount;
    private SAMFileHeader samFileHeader;

    private TaskAttemptContext taskAttemptContext;
    private static Configuration conf;

    // CRAM output class that writes a header before records
    static class CRAMTestWithHeaderOutputFormat
            extends KeyIgnoringCRAMOutputFormat<NullWritable> {
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

    // CRAM Output class that doesn't write a header out before records
    static class CRAMTestNoHeaderOutputFormat
            extends KeyIgnoringCRAMOutputFormat<NullWritable> {
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

        testCRAMFileName = ClassLoader.getSystemClassLoader()
                .getResource("test.cram").getFile();
        testReferenceFileName = ClassLoader.getSystemClassLoader()
                .getResource("auxf.fa").getFile();
        testReferenceSource = new ReferenceSource(Paths.get(testReferenceFileName));

        conf.set("mapred.input.dir", "file://" + testCRAMFileName);
        conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY,
                "file://" + testReferenceFileName);

        // fetch the SAMFile header from the original input to get the
        // expected count
        expectedRecordCount = getCRAMRecordCount(new File(testCRAMFileName));
        samFileHeader = SAMHeaderReader.readSAMHeaderFrom(
                new Path(testCRAMFileName), conf);

        taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
    }

    @Test
    public void testCRAMRecordWriterNoHeader() throws Exception {
        final File outFile = File.createTempFile("testCRAMWriter", ".cram");
        outFile.deleteOnExit();
        final Path outPath = new Path(outFile.toURI());

        final CRAMTestNoHeaderOutputFormat cramOut = new CRAMTestNoHeaderOutputFormat();
        conf.set(CRAMTestNoHeaderOutputFormat.READ_HEADER_FROM_FILE, testCRAMFileName);

        RecordWriter<NullWritable, SAMRecordWritable> rw =
                cramOut.getRecordWriter(taskAttemptContext, outPath);

        final SamReader samReader = SamReaderFactory.makeDefault()
                .referenceSequence(new File(testReferenceFileName))
                .open(new File(testCRAMFileName));

        for (final SAMRecord r : samReader) {
            final SAMRecordWritable samRW = new SAMRecordWritable();
            samRW.set(r);
            rw.write(null, samRW);
        }
        samReader.close();
        rw.close(taskAttemptContext);

        // now verify the container stream
        final int actualCount = getCRAMRecordCount(outFile, samFileHeader,
            testReferenceSource);
        assertEquals(expectedRecordCount, actualCount);
    }

    @Test
    public void testCRAMRecordWriterWithHeader() throws Exception {
        final File outFile = File.createTempFile("testCRAMWriter", ".cram");
        outFile.deleteOnExit();
        final Path outPath = new Path(outFile.toURI());

        final CRAMTestWithHeaderOutputFormat cramOut = new CRAMTestWithHeaderOutputFormat();
        conf.set(CRAMTestNoHeaderOutputFormat.READ_HEADER_FROM_FILE, testCRAMFileName);

        RecordWriter<NullWritable, SAMRecordWritable> rw =
                cramOut.getRecordWriter(taskAttemptContext, outPath);

        final SamReader samReader = SamReaderFactory.makeDefault()
                .referenceSequence(new File(testReferenceFileName))
                .open(new File(testCRAMFileName));

        for (final SAMRecord r : samReader) {
            final SAMRecordWritable samRW = new SAMRecordWritable();
            samRW.set(r);
            rw.write(null, samRW);
        }
        samReader.close();
        rw.close(taskAttemptContext);

        // now verify the container stream
        final int actualCount = getCRAMRecordCount(outFile);
        assertEquals(expectedRecordCount, actualCount);
    }

    @Test
    public void testCRAMOutput() throws Exception {
        final Path outputPath = doMapReduce(testCRAMFileName);
        final File outFile = File.createTempFile("testCRAMWriter", ".cram");
        outFile.deleteOnExit();
        SAMFileMerger.mergeParts(outputPath.toUri().toString(), outFile.toURI().toString(),
            SAMFormat.CRAM, samFileHeader);
        final File containerStreamFile =
                new File(new File(outputPath.toUri()), "part-m-00000");
        final int actualCount = getCRAMRecordCount(outFile);
        assertEquals(expectedRecordCount, actualCount);
    }

    @Test
    public void testCRAMRoundTrip() throws Exception {
        // run a m/r job to write out a cram file
        Path outputPath = doMapReduce(testCRAMFileName);

        // merge the parts, and write to a temp file
        final File outFile = File.createTempFile("testCRAMWriter", ".cram");
        outFile.deleteOnExit();
        SAMFileMerger.mergeParts(outputPath.toUri().toString(), outFile.toURI().toString(),
            SAMFormat.CRAM, samFileHeader);

        // now use the assembled output as m/r input
        outputPath = doMapReduce(outFile.getAbsolutePath());

        // merge the parts again
        SAMFileMerger.mergeParts(outputPath.toUri().toString(), outFile.toURI().toString(),
            SAMFormat.CRAM, samFileHeader);

        // verify the final output
        final int actualCount = getCRAMRecordCount(outFile);
        assertEquals(expectedRecordCount, actualCount);
    }

    private Path doMapReduce(final String inputFile) throws Exception {
        final FileSystem fileSystem = FileSystem.get(conf);
        final Path inputPath = new Path(inputFile);
        final Path outputPath = fileSystem.makeQualified(new Path("target/out"));
        fileSystem.delete(outputPath, true);

        final Job job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, inputPath);

        job.setInputFormatClass(CRAMInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(SAMRecordWritable.class);

        conf.set(CRAMTestNoHeaderOutputFormat.READ_HEADER_FROM_FILE, inputFile);
        job.setOutputFormatClass(CRAMTestNoHeaderOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(SAMRecordWritable.class);

        job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, outputPath);

        final boolean success = job.waitForCompletion(true);
        assertTrue(success);

        return outputPath;
    }

    private int getCRAMRecordCount(final File cramFile) {
        final CRAMFileReader cramReader =
            new CRAMFileReader(cramFile,
                (File)null,
                testReferenceSource);
        final Iterator<SAMRecord> it = cramReader.getIterator();
        int recCount = 0;
        while (it.hasNext()) {
            it.next();
            recCount++;
        }
        cramReader.close();
        return recCount;
    }

    private int getCRAMRecordCount(
        final File containerStreamFile,
        final SAMFileHeader header,
        final ReferenceSource refSource) throws IOException
    {
        // assemble a proper CRAM file from the container stream shard(s) in
        // order to verify the contents
        final ByteArrayInputStream mergedStream = mergeCRAMContainerStream (
            containerStreamFile,
            header,
            refSource
        );

        // now we can verify that we can read everything back in
        final CRAMFileReader resultCRAMReader = new CRAMFileReader(
            mergedStream,
            (SeekableStream) null,
            refSource,
            ValidationStringency.DEFAULT_STRINGENCY);
        final Iterator<SAMRecord> it = resultCRAMReader.getIterator();
        int actualCount = 0;
        while (it.hasNext()) {
            it.next();
            actualCount++;
        }
        return actualCount;
    }

    // TODO: SAMOutputPreparer knows how to prepare the beginning of a stream,
    // but not how to populate or terminate it (which for CRAM requires a special
    // terminating EOF container). For now we'll use SAMPreparer here so we get
    // some test coverage, and then manually populate and terminate, but we
    // should consolidate/refactor the knowledge of how to do this aggregation
    // for each output type in one place in a separate PR
    // https://github.com/HadoopGenomics/Hadoop-BAM/issues/61
    private ByteArrayInputStream mergeCRAMContainerStream(
        final File containerStreamFile,
        final SAMFileHeader header,
        final ReferenceSource refSource) throws IOException
    {
        // assemble a proper CRAM file from the container stream shard(s) in
        // order to verify the contents
        final ByteArrayOutputStream cramOutputStream = new ByteArrayOutputStream();
        // write out the cram file header
        new SAMOutputPreparer().prepareForRecords(
            cramOutputStream,
            SAMFormat.CRAM,
            header);
        // now copy the contents of the container stream shard(s) written out by
        // the M/R job
        final ByteArrayOutputStream containerOutputStream = new ByteArrayOutputStream();
        Files.copy(containerStreamFile.toPath(), containerOutputStream);
        containerOutputStream.writeTo(cramOutputStream);

        // use containerStreamWriter directly to properly terminate the output
        // stream with an EOF container
        final CRAMContainerStreamWriter containerStreamWriter =
            new CRAMContainerStreamWriter(
                cramOutputStream,
                null,
                refSource,
                header,
                "CRAMTest");
        containerStreamWriter.finish(true); // close and write an EOF container
        cramOutputStream.close();

        return new ByteArrayInputStream(cramOutputStream.toByteArray());
    }
}
