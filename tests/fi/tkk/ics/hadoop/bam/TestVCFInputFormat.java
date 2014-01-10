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

package tests.fi.tkk.ics.hadoop.bam;

import fi.tkk.ics.hadoop.bam.VCFInputFormat;
import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import hbparquet.hadoop.util.ContextUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.broadinstitute.variant.variantcontext.VariantContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static org.mockito.Mockito.mock;

public class TestVCFInputFormat {
    private VariantContextWritable writable;
    private RecordReader<LongWritable, VariantContextWritable> reader;
    private TaskAttemptContext taskAttemptContext;

    @Before
    public void setup() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, InterruptedException, NoSuchFieldException {
        Configuration conf = new Configuration();
        String input_file = ClassLoader.getSystemClassLoader().getResource("tests/resources/test.vcf").getFile();
        conf.set("hadoopbam.vcf.trust-exts", "true");
        conf.set("mapred.input.dir", "file://" + input_file);

        taskAttemptContext = ContextUtil.newTaskAttemptContext(conf, mock(TaskAttemptID.class));
        JobContext ctx = ContextUtil.newJobContext(conf, taskAttemptContext.getJobID());


        Job job = new Job(conf, "test");
        Path file = new Path("file://" + input_file);
        VCFInputFormat inputFormat = new VCFInputFormat(conf);
        List<InputSplit> splits = inputFormat.getSplits(ctx);
        reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);
        reader.initialize(splits.get(0), taskAttemptContext);
    }

    @Test
    public void testSimple() throws Exception {
        if (!reader.nextKeyValue())
            throw new Exception("could not read first VariantContext");

        writable = reader.getCurrentValue();
        assert (writable != null && writable.get() != null);

        VariantContext vc = writable.get();

        assert (vc.getChr().equals("20"));
        assert (vc.getStart() == 14370 && vc.getEnd() == 14370);
        assert (vc.getReference().getBaseString().equals("G"));
        assert (vc.getAlternateAllele(0).getBaseString().equals("A"));

        if (!reader.nextKeyValue())
            throw new Exception("could not read second VariantContext");

        writable = reader.getCurrentValue();
        assert (writable != null && writable.get() != null);

        vc = writable.get();

        assert (vc.getChr().equals("20"));
        assert (vc.getStart() == 17330 && vc.getEnd() == 17330);
        assert (vc.getReference().getBaseString().equals("T"));
        assert (vc.getAlternateAllele(0).getBaseString().equals("A"));
    }
}
