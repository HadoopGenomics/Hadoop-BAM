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

import fi.tkk.ics.hadoop.bam.KeyIgnoringVCFOutputFormat;
import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import net.sf.samtools.seekablestream.SeekableFileStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.broadinstitute.variant.variantcontext.Allele;
import org.broadinstitute.variant.variantcontext.GenotypesContext;
import org.broadinstitute.variant.variantcontext.VariantContext;
import org.broadinstitute.variant.variantcontext.VariantContextBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import static org.mockito.Mockito.mock;

public class TestVCFOutputFormat {
    private VariantContextWritable writable;
    private RecordWriter<Long, VariantContextWritable> writer;
    private TaskAttemptContext taskAttemptContext;
    private String test_vcf_output = "/tmp/test_vcf_output";

    @Before
    public void setup() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        writable = new VariantContextWritable();

        Configuration conf = new Configuration();
        conf.set("hadoopbam.vcf.output-format", "VCF");
        KeyIgnoringVCFOutputFormat<Long> outputFormat = new KeyIgnoringVCFOutputFormat<Long>(conf);
        String header_file = ClassLoader.getSystemClassLoader().getResource("tests/resources/test.vcf").getFile();
        outputFormat.readHeaderFrom(new SeekableFileStream(new File(header_file)));
        taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
        writer = outputFormat.getRecordWriter(taskAttemptContext, new Path("file://" + test_vcf_output));
    }

    private void skipHeader(LineNumberReader reader) throws IOException {
        String line = reader.readLine();

        while (line.startsWith("#")) {
            reader.mark(1000);
            line = reader.readLine();
        }

        reader.reset();
    }

    @Test
    public void testSimple() throws Exception {
        VariantContextBuilder vctx_builder = new VariantContextBuilder();

        ArrayList<Allele> alleles = new ArrayList<Allele>();
        alleles.add(Allele.create("A", false));
        alleles.add(Allele.create("C", true));
        vctx_builder.alleles(alleles);

        GenotypesContext genotypes = GenotypesContext.NO_GENOTYPES;
        vctx_builder.genotypes(genotypes);

        HashSet<String> filters = new HashSet<String>();
        vctx_builder.filters(filters);

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("NS", new Integer(4));
        vctx_builder.attributes(attributes);

        vctx_builder.loc("20", 2, 2);
        vctx_builder.log10PError(-8.0);

        String[] expected = new String[]{"20", "2", ".", "C", "A", "80", "PASS", "NS=4"};

        VariantContext ctx = vctx_builder.make();
        writable.set(ctx);
        writer.write(1L, writable);
        writer.close(taskAttemptContext);

        LineNumberReader reader = new LineNumberReader(new FileReader(test_vcf_output));
        skipHeader(reader);
        String[] fields = Arrays.copyOf(reader.readLine().split("\t"), expected.length);
        Assert.assertArrayEquals("comparing VCF single line", expected, fields);
    }
}
