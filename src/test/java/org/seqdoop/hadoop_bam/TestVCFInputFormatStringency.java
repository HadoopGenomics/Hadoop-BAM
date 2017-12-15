// Copyright (c) 2017 Aalto University
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

import htsjdk.samtools.ValidationStringency;
import htsjdk.tribble.TribbleException;
import htsjdk.variant.variantcontext.VariantContext;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class TestVCFInputFormatStringency {

    public void checkReading(ValidationStringency validationStringency) throws Exception {
        String filename = "invalid_info_field.vcf";
        Configuration conf = new Configuration();
        String input_file = ClassLoader.getSystemClassLoader().getResource(filename).getFile();
        conf.set("mapred.input.dir", "file://" + input_file);

        if (validationStringency != null) {
            VCFRecordReader.setValidationStringency(conf, validationStringency);
        }

        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
        JobContext ctx = new JobContextImpl(conf, taskAttemptContext.getJobID());

        VCFInputFormat inputFormat = new VCFInputFormat(conf);
        List<InputSplit> splits = inputFormat.getSplits(ctx);
        assertEquals(1, splits.size());
        RecordReader<LongWritable, VariantContextWritable> reader =
            inputFormat.createRecordReader(splits.get(0), taskAttemptContext);
        int counter = 0;
        while (reader.nextKeyValue()) {
            VariantContextWritable writable = reader.getCurrentValue();
            assertNotNull(writable);
            VariantContext vc = writable.get();
            assertNotNull(vc);
            String value = vc.toString();
            assertNotNull(value);
            counter++;
        }
        assertEquals(4, counter);
    }

    @Test(expected = TribbleException.class)
    public void testUnset() throws Exception {
        checkReading(null); // defaults to strict
    }

    @Test(expected = TribbleException.class)
    public void testDefault() throws Exception {
        checkReading(ValidationStringency.DEFAULT_STRINGENCY); // defaults to strict
    }

    @Test
    public void testSilent() throws Exception {
        checkReading(ValidationStringency.SILENT);
    }

    @Test
    public void testLenient() throws Exception {
        checkReading(ValidationStringency.LENIENT);
    }

    @Test(expected = TribbleException.class)
    public void testStrict() throws Exception {
        checkReading(ValidationStringency.STRICT);
    }
}
