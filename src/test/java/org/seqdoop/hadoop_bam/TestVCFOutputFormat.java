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

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.variant.variantcontext.*;
import htsjdk.variant.vcf.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.seqdoop.hadoop_bam.util.VCFHeaderReader;

import static org.mockito.Mockito.mock;

public class TestVCFOutputFormat {
    private VariantContextWritable writable;
    private RecordWriter<Long, VariantContextWritable> writer;
    private TaskAttemptContext taskAttemptContext;
    private File test_vcf_output;

    @Before
    public void setup() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        test_vcf_output = File.createTempFile("test_vcf_output", "");
        test_vcf_output.delete();
        writable = new VariantContextWritable();
        Configuration conf = new Configuration();
        conf.set("hadoopbam.vcf.output-format", "VCF");
        KeyIgnoringVCFOutputFormat<Long> outputFormat = new KeyIgnoringVCFOutputFormat<Long>(conf);
        outputFormat.setHeader(readHeader());
        taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
        writer = outputFormat.getRecordWriter(taskAttemptContext, new Path("file://" + test_vcf_output));
    }

    @After
    public void cleanup() throws IOException {
        FileUtil.fullyDelete(test_vcf_output);
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
    
    @Test
    public void testVariantContextReadWrite() throws IOException, InterruptedException
    {
        // This is to check whether issue https://github.com/HadoopGenomics/Hadoop-BAM/issues/1 has been
        // resolved
    	VariantContextBuilder vctx_builder = new VariantContextBuilder();

        ArrayList<Allele> alleles = new ArrayList<Allele>();
        alleles.add(Allele.create("C", false));
        alleles.add(Allele.create("G", true));
        vctx_builder.alleles(alleles);

        ArrayList<Genotype> genotypes = new ArrayList<Genotype>();
        GenotypeBuilder builder = new GenotypeBuilder();
        genotypes.add(builder.alleles(alleles.subList(0, 1)).name("NA00001").GQ(48).DP(1).make());
        genotypes.add(builder.alleles(alleles.subList(0, 1)).name("NA00002").GQ(42).DP(2).make());
        genotypes.add(builder.alleles(alleles.subList(0, 1)).name("NA00003").GQ(39).DP(3).make());
        vctx_builder.genotypes(genotypes);

        HashSet<String> filters = new HashSet<String>();
        vctx_builder.filters(filters);

        HashMap<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("NS", new Integer(4));
        vctx_builder.attributes(attributes);

        vctx_builder.loc("20", 2, 2);
        vctx_builder.log10PError(-8.0);

        VariantContext ctx = vctx_builder.make();
        VariantContextWithHeader ctxh = new VariantContextWithHeader(ctx, readHeader());
        writable.set(ctxh);

        DataOutputBuffer out = new DataOutputBuffer(1000);
        writable.write(out);
        
        byte[] data = out.getData();
        ByteArrayInputStream bis = new ByteArrayInputStream(data);

        writable = new VariantContextWritable();
        writable.readFields(new DataInputStream(bis));

        VariantContext vc = writable.get();
        Assert.assertArrayEquals("comparing Alleles",ctx.getAlleles().toArray(),vc.getAlleles().toArray());
        Assert.assertEquals("comparing Log10PError",ctx.getLog10PError(),vc.getLog10PError(),0.01);
        Assert.assertArrayEquals("comparing Filters",ctx.getFilters().toArray(),vc.getFilters().toArray());
        Assert.assertEquals("comparing Attributes",ctx.getAttributes(),vc.getAttributes());

        // Now check the genotypes. Note: we need to make the header accessible before decoding the genotypes.
        GenotypesContext gc = vc.getGenotypes();
        assert(gc instanceof LazyVCFGenotypesContext);
        LazyVCFGenotypesContext.HeaderDataCache headerDataCache = new LazyVCFGenotypesContext.HeaderDataCache();
        headerDataCache.setHeader(readHeader());
        ((LazyVCFGenotypesContext) gc).getParser().setHeaderDataCache(headerDataCache);

        for (Genotype genotype : genotypes) {
            Assert.assertEquals("checking genotype name", genotype.getSampleName(), gc.get(genotypes.indexOf(genotype)).getSampleName());
            Assert.assertEquals("checking genotype quality", genotype.getGQ(), gc.get(genotypes.indexOf(genotype)).getGQ());
            Assert.assertEquals("checking genotype read depth", genotype.getDP(), gc.get(genotypes.indexOf(genotype)).getDP());
        }
    }

    private VCFHeader readHeader() throws IOException {
        String header_file = ClassLoader.getSystemClassLoader().getResource("test.vcf").getFile();
        VCFHeader header = VCFHeaderReader.readHeaderFrom(new SeekableFileStream(new File(header_file)));
        return header;
    }
}
