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

package org.seqdoop.hadoop_bam.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import htsjdk.variant.variantcontext.VariantContext;

import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VCFOutputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

/**
 * Simple example that reads a VCF file, groups variants by their ID and writes the
 * output again as VCF file.
 *
 * Usage: hadoop jar target/*-jar-with-dependencies.jar org.seqdoop.hadoop_bam.examples.TestVCF \
 *     <input.vcf> <output_directory>
 */
public class TestVCF extends Configured implements Tool {

    static class MyVCFOutputFormat
            extends FileOutputFormat<Text, VariantContextWritable> {
        public static final String INPUT_PATH_PROP = "testvcf.input_path";

        private KeyIgnoringVCFOutputFormat<Text> baseOF;

        private void initBaseOF(Configuration conf) {
            if (baseOF == null)
                baseOF = new KeyIgnoringVCFOutputFormat<Text>(conf);
        }

        @Override
        public RecordWriter<Text, VariantContextWritable> getRecordWriter(
                TaskAttemptContext context)
                throws IOException {
            final Configuration conf = context.getConfiguration();
            initBaseOF(conf);

            if (baseOF.getHeader() == null) {
                final Path p = new Path(conf.get(INPUT_PATH_PROP));
                baseOF.readHeaderFrom(p, p.getFileSystem(conf));
            }

            return baseOF.getRecordWriter(context, getDefaultWorkFile(context, ""));
        }
    }

    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();

        conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, "VCF");
        conf.set(MyVCFOutputFormat.INPUT_PATH_PROP, args[0]);

        final Job job = new Job(conf);

        job.setJarByClass(TestVCF.class);
        job.setMapperClass (TestVCFMapper.class);
        job.setReducerClass(TestVCFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VariantContextWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VariantContextWritable.class);

        job.setInputFormatClass(VCFInputFormat.class);
        job.setOutputFormatClass(MyVCFOutputFormat.class);

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));

        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.submit();

        if (!job.waitForCompletion(true)) {
            System.err.println("sort :: Job failed.");
            return 1;
        }

        return 0;
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: hadoop jar <name.jar> %s <input.vcf> <output_directory>\n", TestVCF.class.getCanonicalName());
            System.exit(0);
        }
        int res = ToolRunner.run(new Configuration(), new TestVCF(), args);
        System.exit(res);
    }
}

final class TestVCFMapper
        extends org.apache.hadoop.mapreduce.Mapper<LongWritable,VariantContextWritable, Text, VariantContextWritable>
{
    @Override protected void map(
            LongWritable ignored, VariantContextWritable wrec,
            org.apache.hadoop.mapreduce.Mapper<LongWritable, VariantContextWritable, Text, VariantContextWritable>.Context
                    ctx)
            throws InterruptedException, IOException
    {
        final VariantContext context = wrec.get();
        System.out.println(context.toString());
        ctx.write(new Text(context.getChr()+":" + context.getID()), wrec);
    }
}

final class TestVCFReducer
        extends org.apache.hadoop.mapreduce.Reducer<Text,VariantContextWritable, Text, VariantContextWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<VariantContextWritable> records,
            org.apache.hadoop.mapreduce.Reducer<Text, VariantContextWritable, Text, VariantContextWritable>.Context ctx)
            throws IOException, InterruptedException {

        final Iterator<VariantContextWritable> it = records.iterator();

        while (it.hasNext()) {
            VariantContextWritable a = it.next();
            System.out.println("writing; " + a.get().toString());
            ctx.write(key, a);
        }
    }
}

