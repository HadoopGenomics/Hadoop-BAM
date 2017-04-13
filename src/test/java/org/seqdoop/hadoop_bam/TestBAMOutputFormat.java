package org.seqdoop.hadoop_bam;

import htsjdk.samtools.*;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.seqdoop.hadoop_bam.util.SAMFileMerger;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

import java.io.*;
import java.util.Iterator;
import org.seqdoop.hadoop_bam.util.SAMOutputPreparer;

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
        expectedRecordCount = getBAMRecordCount(new File(testBAMFileName));
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
        final int actualCount = getBAMRecordCount(outFile, samFileHeader);
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
        final int actualCount = getBAMRecordCount(outFile);
        assertEquals(expectedRecordCount, actualCount);
    }

    @Test
    public void testBAMOutput() throws Exception {
        final Path outputPath = doMapReduce(testBAMFileName);
        final File outFile = File.createTempFile("testBAMWriter", ".bam");
        outFile.deleteOnExit();
        SAMFileMerger.mergeParts(outputPath.toUri().toString(), outFile.toURI().toString(),
            SAMFormat.BAM, samFileHeader);
        final int actualCount = getBAMRecordCount(outFile);
        assertEquals(expectedRecordCount, actualCount);
    }

    @Test
    public void testEmptyBAM() throws Exception {
        String bam = BAMTestUtil.writeBamFile(0,
            SAMFileHeader.SortOrder.coordinate).toURI().toString();
        conf.setBoolean(BAMOutputFormat.WRITE_SPLITTING_BAI, true);
        final Path outputPath = doMapReduce(bam);
        final File outFile = File.createTempFile("testBAMWriter", ".bam");
        outFile.deleteOnExit();
        SAMFileMerger.mergeParts(outputPath.toUri().toString(), outFile.toURI().toString(),
            SAMFormat.BAM, new SAMRecordSetBuilder(true, SAMFileHeader.SortOrder.coordinate).getHeader());
        final int actualCount = getBAMRecordCount(outFile);
        assertEquals(0, actualCount);
    }

    @Test
    public void testBAMWithSplittingBai() throws Exception {
        int numPairs = 20000;
        // create a large BAM with lots of index points
        String bam = BAMTestUtil.writeBamFile(20000,
            SAMFileHeader.SortOrder.coordinate).toURI().toString();
        conf.setInt(FileInputFormat.SPLIT_MAXSIZE, 800000); // force multiple parts
        conf.setBoolean(BAMOutputFormat.WRITE_SPLITTING_BAI, true);
        final Path outputPath = doMapReduce(bam);

        List<SAMRecord> recordsAtSplits = new ArrayList<>();
        File[] splittingIndexes = new File(outputPath.toUri()).listFiles(pathname -> {
            return pathname.getName().endsWith(SplittingBAMIndexer.OUTPUT_FILE_EXTENSION);
        });
        Arrays.sort(splittingIndexes); // ensure files are sorted by name
        for (File file : splittingIndexes) {
            File bamFile = new File(file.getParentFile(),
                file.getName().replace(SplittingBAMIndexer.OUTPUT_FILE_EXTENSION, ""));
            SplittingBAMIndex index = new SplittingBAMIndex(file);
            recordsAtSplits.addAll(getRecordsAtSplits(bamFile, index));
        }

        final File outFile = File.createTempFile("testBAMWriter", ".bam");
        //outFile.deleteOnExit();
        SAMFileMerger.mergeParts(outputPath.toUri().toString(), outFile.toURI().toString(),
            SAMFormat.BAM,
            new SAMRecordSetBuilder(true, SAMFileHeader.SortOrder.coordinate).getHeader());

        final int actualCount = getBAMRecordCount(outFile);
        assertEquals(numPairs * 2 + 2, actualCount); // 2 unmapped reads

        File splittingBai = new File(outFile.getParentFile(), outFile.getName() +
            SplittingBAMIndexer.OUTPUT_FILE_EXTENSION);
        SplittingBAMIndex splittingBAMIndex = new SplittingBAMIndex(splittingBai);

        assertEquals(recordsAtSplits, getRecordsAtSplits(outFile, splittingBAMIndex));
    }

    private List<SAMRecord> getRecordsAtSplits(File bam, SplittingBAMIndex index) throws IOException {
        List<SAMRecord> records = new ArrayList<>();
        BAMRecordCodec codec = new BAMRecordCodec(samFileHeader);
        BlockCompressedInputStream bci = new BlockCompressedInputStream(bam);
        codec.setInputStream(bci);
        for (Long offset : index.getVirtualOffsets()) {
            bci.seek(offset);
            SAMRecord record = codec.decode();
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    @Test
    public void testBAMRoundTrip() throws Exception {
        // run a m/r job to write out a bam file
        Path outputPath = doMapReduce(testBAMFileName);

        // merge the parts, and write to a temp file
        final File outFile = File.createTempFile("testBAMWriter", ".bam");
        outFile.deleteOnExit();
        SAMFileMerger.mergeParts(outputPath.toUri().toString(), outFile.toURI().toString(),
            SAMFormat.BAM, samFileHeader);

        // now use the assembled output as m/r input
        outputPath = doMapReduce(outFile.getAbsolutePath());

        // merge the parts again
        SAMFileMerger.mergeParts(outputPath.toUri().toString(), outFile.toURI().toString(),
            SAMFormat.BAM, samFileHeader);

        // verify the final output
        final int actualCount = getBAMRecordCount(outFile);
        assertEquals(expectedRecordCount, actualCount);
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

    private int getBAMRecordCount(final File bamFile) throws IOException {
        final SamReader bamReader = SamReaderFactory.makeDefault()
                                        .open(SamInputResource.of(bamFile));
        final Iterator<SAMRecord> it = bamReader.iterator();
        int recCount = 0;
        while (it.hasNext()) {
            it.next();
            recCount++;
        }
        bamReader.close();
        return recCount;
    }

    private int getBAMRecordCount(
        final File blockStreamFile,
        final SAMFileHeader header) throws IOException
    {
        // assemble a proper BAM file from the block stream shard(s) in
        // order to verify the contents
        final ByteArrayInputStream mergedStream = mergeBAMBlockStream (
            blockStreamFile,
            header
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
        final SAMFileHeader header) throws IOException
    {
        // assemble a proper BAM file from the block stream shard(s) in
        // order to verify the contents
        final ByteArrayOutputStream bamOutputStream = new ByteArrayOutputStream();

        // write out the bam file header
        new SAMOutputPreparer().prepareForRecords(
            bamOutputStream,
            SAMFormat.BAM,
            header);

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
