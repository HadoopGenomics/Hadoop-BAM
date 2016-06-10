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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import htsjdk.samtools.util.Interval;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.seqdoop.hadoop_bam.util.BGZFCodec;
import org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class TestVCFInputFormat {
    enum NUM_SPLITS {
        ANY, EXACTLY_ONE, MORE_THAN_ONE
    }
    private String filename;
    private NUM_SPLITS expectedSplits;
    private Interval interval;
    private VariantContextWritable writable;
    private List<RecordReader<LongWritable, VariantContextWritable>> readers;
    private TaskAttemptContext taskAttemptContext;

    public TestVCFInputFormat(String filename, NUM_SPLITS expectedSplits, Interval interval) {
        this.filename = filename;
        this.expectedSplits = expectedSplits;
        this.interval = interval;
    }

    @Parameterized.Parameters
    public static Collection<Object> data() {
        return Arrays.asList(new Object[][] {
            {"test.vcf", NUM_SPLITS.ANY, null},
            {"test.vcf.gz", NUM_SPLITS.EXACTLY_ONE, null},
            {"test.vcf.bgzf.gz", NUM_SPLITS.ANY, null},
            // BCF tests currently fail due to https://github.com/samtools/htsjdk/issues/507
//            {"test.uncompressed.bcf", NUM_SPLITS.ANY, null},
//            {"test.bgzf.bcf", NUM_SPLITS.ANY, null},
            {"HiSeq.10000.vcf", NUM_SPLITS.MORE_THAN_ONE, null},
            {"HiSeq.10000.vcf.gz", NUM_SPLITS.EXACTLY_ONE, null},
            {"HiSeq.10000.vcf.bgzf.gz", NUM_SPLITS.MORE_THAN_ONE, null},
            {"HiSeq.10000.vcf.bgzf.gz", NUM_SPLITS.EXACTLY_ONE,
                new Interval("chr1", 2700000, 2800000)}, // chosen to fall in one split
            {"HiSeq.10000.vcf.bgz", NUM_SPLITS.MORE_THAN_ONE, null},
            {"HiSeq.10000.vcf.bgz", NUM_SPLITS.EXACTLY_ONE,
                new Interval("chr1", 2700000, 2800000)} // chosen to fall in one split
        });
    }

    @Before
    public void setup() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, InterruptedException, NoSuchFieldException {
        Configuration conf = new Configuration();
        String input_file = ClassLoader.getSystemClassLoader().getResource(filename).getFile();
        conf.set("hadoopbam.vcf.trust-exts", "true");
        conf.set("mapred.input.dir", "file://" + input_file);
        conf.setStrings("io.compression.codecs", BGZFEnhancedGzipCodec.class.getCanonicalName(),
            BGZFCodec.class.getCanonicalName());
        conf.setInt(FileInputFormat.SPLIT_MAXSIZE, 100 * 1024); // 100K

        if (interval != null) {
            VCFInputFormat.setIntervals(conf, ImmutableList.of(interval));
        }

        taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
        JobContext ctx = new JobContextImpl(conf, taskAttemptContext.getJobID());

        VCFInputFormat inputFormat = new VCFInputFormat(conf);
        List<InputSplit> splits = inputFormat.getSplits(ctx);
        switch (expectedSplits) {
            case EXACTLY_ONE:
                assertEquals("Should be exactly one split", 1, splits.size());
                break;
            case MORE_THAN_ONE:
                assertTrue("Should be more than one split", splits.size() > 1);
                break;
            case ANY:
            default:
                break;
        }
        readers = new ArrayList<>();
        for (InputSplit split : splits) {
            RecordReader<LongWritable, VariantContextWritable> reader = inputFormat.createRecordReader(split, taskAttemptContext);
            reader.initialize(split, taskAttemptContext);
            readers.add(reader);
        }
    }

    @Test
    public void countEntries() throws Exception {
        VCFFileReader vcfFileReader =
            new VCFFileReader(new File("src/test/resources/" + filename), false);
        Iterator<VariantContext> variantIterator;
        if (interval == null) {
            variantIterator = vcfFileReader.iterator();
        } else {
            variantIterator = vcfFileReader.query(interval.getContig(),
                interval.getStart(), interval.getEnd());
        }
        int expectedCount = Iterators.size(variantIterator);

        int counter = 0;
        for (RecordReader<LongWritable, VariantContextWritable> reader : readers) {
            while (reader.nextKeyValue()) {
                writable = reader.getCurrentValue();
                assertNotNull(writable);
                VariantContext vc = writable.get();
                assertNotNull(vc);
                String value = vc.toString();
                assertNotNull(value);
                counter++;
            }
        }
        assertEquals(expectedCount, counter);
    }

    @Test
    public void testFirstSecond() throws Exception {
        if (!filename.startsWith("test.")) {
            return;
        }
        RecordReader<LongWritable, VariantContextWritable> reader = readers.get(0);
        if (!reader.nextKeyValue())
            throw new Exception("could not read first VariantContext");

        writable = reader.getCurrentValue();
        assertNotNull(writable);
        VariantContext vc = writable.get();
        assertNotNull(vc);

        assertEquals("20", vc.getContig());
        assertEquals(14370, vc.getStart());
        assertEquals(14370, vc.getEnd());
        assertEquals("G", vc.getReference().getBaseString());
        assertEquals("A", vc.getAlternateAllele(0).getBaseString());

        assertTrue("second VariantContext", reader.nextKeyValue());

        writable = reader.getCurrentValue();
        assertNotNull(writable);
        vc = writable.get();
        assertNotNull(vc);

        assertEquals("20", vc.getContig());
        assertEquals(17330, vc.getStart());
        assertEquals(17330, vc.getEnd());
        assertEquals("T", vc.getReference().getBaseString());
        assertEquals("A", vc.getAlternateAllele(0).getBaseString());
    }
}
