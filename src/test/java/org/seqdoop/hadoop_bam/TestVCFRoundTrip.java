// Copyright (c) 2013 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package org.seqdoop.hadoop_bam;

import com.google.common.collect.Iterators;
import com.google.common.io.Files;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextComparator;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.seqdoop.hadoop_bam.TestVCFInputFormat.NUM_SPLITS;
import org.seqdoop.hadoop_bam.util.BGZFCodec;
import org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec;
import org.seqdoop.hadoop_bam.util.VCFFileMerger;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestVCFRoundTrip {

    // VCF output format that writes a header before records
    static class VCFTestWithHeaderOutputFormat
        extends KeyIgnoringVCFOutputFormat<NullWritable> {
        public final static String READ_HEADER_FROM_FILE = "TestVCF.header";

        public VCFTestWithHeaderOutputFormat() {
            super(VCFFormat.VCF);
        }

        @Override
        public RecordWriter<NullWritable, VariantContextWritable> getRecordWriter(
            TaskAttemptContext ctx) throws IOException {
            Path vcfPath = new Path(conf.get(READ_HEADER_FROM_FILE));
            readHeaderFrom(vcfPath, vcfPath.getFileSystem(conf));
            return super.getRecordWriter(ctx);
        }
    }

    // VCF output format that doesn't write a header before records
    static class VCFTestNoHeaderOutputFormat
        extends KeyIgnoringVCFOutputFormat<NullWritable> {
        public final static String READ_HEADER_FROM_FILE = "TestVCF.header";

        public VCFTestNoHeaderOutputFormat() {
            super(VCFFormat.VCF);
        }

        @Override
        public RecordWriter<NullWritable, VariantContextWritable> getRecordWriter(
            TaskAttemptContext ctx) throws IOException {
            Path vcfPath = new Path(conf.get(READ_HEADER_FROM_FILE));
            readHeaderFrom(vcfPath, vcfPath.getFileSystem(conf));
            ctx.getConfiguration().setBoolean(WRITE_HEADER_PROPERTY, false);
            return super.getRecordWriter(ctx);
        }
    }

    @Parameterized.Parameters
    public static Collection<Object> data() {
        return Arrays.asList(new Object[][] {
            {"test.vcf", null, NUM_SPLITS.ANY},
            {"test.vcf.gz", BGZFEnhancedGzipCodec.class, NUM_SPLITS.EXACTLY_ONE},
            {"test.vcf.bgzf.gz", BGZFCodec.class, NUM_SPLITS.ANY},
            {"test.vcf.bgz", BGZFCodec.class, NUM_SPLITS.ANY},
            {"HiSeq.10000.vcf", null, NUM_SPLITS.MORE_THAN_ONE},
            {"HiSeq.10000.vcf.gz", BGZFEnhancedGzipCodec.class, NUM_SPLITS.EXACTLY_ONE},
            {"HiSeq.10000.vcf.bgzf.gz", BGZFCodec.class, NUM_SPLITS.MORE_THAN_ONE},
            {"HiSeq.10000.vcf.bgz", BGZFCodec.class, NUM_SPLITS.MORE_THAN_ONE}
        });
    }

    private static Configuration conf;

    private String testVCFFileName;
    private Class<? extends CompressionCodec> codecClass;
    private NUM_SPLITS expectedSplits;

    public TestVCFRoundTrip(String filename, Class<? extends CompressionCodec> codecClass,
        NUM_SPLITS expectedSplits) {
        testVCFFileName = ClassLoader.getSystemClassLoader().getResource(filename).getFile();
        this.codecClass = codecClass;
        this.expectedSplits = expectedSplits;
    }

    @Before
    public void setup() throws Exception {
        conf = new Configuration();
        conf.set(VCFTestWithHeaderOutputFormat.READ_HEADER_FROM_FILE, testVCFFileName);
        conf.setStrings("io.compression.codecs", BGZFCodec.class.getCanonicalName(),
            BGZFEnhancedGzipCodec.class.getCanonicalName());
        conf.setInt(FileInputFormat.SPLIT_MAXSIZE, 100 * 1024); // 100K
    }

    @Test
    public void testRoundTrip() throws Exception {
        Path vcfPath = new Path("file://" + testVCFFileName);

        // run a MR job to write out a VCF file
        Path outputPath = doMapReduce(vcfPath, true);

        // verify the output is the same as the input
        List<VariantContext> expectedVariants = new ArrayList<>();
        VCFFileReader vcfFileReader = parseVcf(new File(testVCFFileName));
        Iterators.addAll(expectedVariants, vcfFileReader.iterator());

        int splits = 0;
        List<VariantContext> actualVariants = new ArrayList<>();
        File[] vcfFiles = new File(outputPath.toUri()).listFiles(
            pathname -> (!pathname.getName().startsWith(".") &&
                !pathname.getName().startsWith("_")));
        Arrays.sort(vcfFiles); // ensure files are sorted by name
        for (File vcf : vcfFiles) {
            splits++;
            Iterators.addAll(actualVariants, parseVcf(vcf).iterator());
            if (BGZFCodec.class.equals(codecClass)) {
                assertTrue(BlockCompressedInputStream.isValidFile(
                    new BufferedInputStream(new FileInputStream(vcf))));
            } else if (BGZFEnhancedGzipCodec.class.equals(codecClass)) {
                assertTrue(VCFFormat.isGzip(
                    new BufferedInputStream(new FileInputStream(vcf))));
            }
        }

        switch (expectedSplits) {
            case EXACTLY_ONE:
                assertEquals("Should be exactly one split", 1, splits);
                break;
            case MORE_THAN_ONE:
                assertTrue("Should be more than one split", splits > 1);
                break;
            case ANY:
            default:
                break;
        }

        // use a VariantContextComparator to check variants are equal
        VCFHeader vcfHeader = VCFHeaderReader.readHeaderFrom(new SeekableFileStream(new
            File(testVCFFileName)));
        VariantContextComparator vcfRecordComparator = vcfHeader.getVCFRecordComparator();
        assertEquals(expectedVariants.size(), actualVariants.size());
        for (int i = 0; i < expectedVariants.size(); i++) {
            assertEquals(0, vcfRecordComparator.compare(expectedVariants.get(i),
                actualVariants.get(i)));
        }
    }

    @Test
    public void testRoundTripWithMerge() throws Exception {
        Path vcfPath = new Path("file://" + testVCFFileName);

        // run a MR job to write out a VCF file
        Path outputPath = doMapReduce(vcfPath, false);

        // merge the output
        VCFHeader vcfHeader = VCFHeaderReader.readHeaderFrom(new SeekableFileStream(new
            File(testVCFFileName)));
        final File outFile = File.createTempFile("testVCFWriter",
            testVCFFileName.substring(testVCFFileName.lastIndexOf(".")));
        outFile.deleteOnExit();
        VCFFileMerger.mergeParts(outputPath.toUri().toString(), outFile.toURI().toString(),
            vcfHeader);
        List<VariantContext> actualVariants = new ArrayList<>();
        VCFFileReader vcfFileReaderActual = parseVcf(outFile);
        Iterators.addAll(actualVariants, vcfFileReaderActual.iterator());

        // verify the output is the same as the input
        List<VariantContext> expectedVariants = new ArrayList<>();
        VCFFileReader vcfFileReader = parseVcf(new File(testVCFFileName));
        Iterators.addAll(expectedVariants, vcfFileReader.iterator());

        // use a VariantContextComparator to check variants are equal
        VariantContextComparator vcfRecordComparator = vcfHeader.getVCFRecordComparator();
        assertEquals(expectedVariants.size(), actualVariants.size());
        for (int i = 0; i < expectedVariants.size(); i++) {
            assertEquals(0, vcfRecordComparator.compare(expectedVariants.get(i),
                actualVariants.get(i)));
        }
    }

    private Path doMapReduce(final Path inputPath, final boolean writeHeader)
        throws Exception {
        final FileSystem fileSystem = FileSystem.get(conf);
        final Path outputPath = fileSystem.makeQualified(new Path("target/out"));
        fileSystem.delete(outputPath, true);

        final Job job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, inputPath);

        job.setInputFormatClass(VCFInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(VariantContextWritable.class);

        job.setOutputFormatClass(writeHeader ? VCFTestWithHeaderOutputFormat.class :
            VCFTestNoHeaderOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VariantContextWritable.class);

        job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, outputPath);
        if (codecClass != null) {
            FileOutputFormat.setOutputCompressorClass(job, codecClass);
        }

        final boolean success = job.waitForCompletion(true);
        assertTrue(success);

        return outputPath;
    }

    private static VCFFileReader parseVcf(File vcf) throws IOException {
        File actualVcf;
        // work around TribbleIndexedFeatureReader not reading header from .bgz files
        if (vcf.getName().endsWith(".bgz")) {
            actualVcf = File.createTempFile(vcf.getName(), ".gz");
            actualVcf.deleteOnExit();
            Files.copy(vcf, actualVcf);
        } else {
            actualVcf = vcf;
        }
        return new VCFFileReader(actualVcf, false);
    }
}
