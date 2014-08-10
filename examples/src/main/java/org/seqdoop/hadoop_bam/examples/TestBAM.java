// Copyright (c) 2014 Aalto University
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import htsjdk.samtools.SAMRecord;

import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

/**
 * Simple example that reads a BAM (or SAM) file, groups reads by their name and writes the
 * output again as BAM file. Note that both the file and its index must be present.
 *
 * Usage: hadoop jar target/*-jar-with-dependencies.jar org.seqdoop.hadoop_bam.examples.TestBAM \
 *     <input.bam> <output_directory>
 */
public class TestBAM extends Configured implements Tool {

  static class MyOutputFormat extends KeyIgnoringBAMOutputFormat<NullWritable> {
      public final static String HEADER_FROM_FILE = "TestBAM.header";

      @Override
      public RecordWriter<NullWritable, SAMRecordWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException {
          final Configuration conf = ctx.getConfiguration();
          readSAMHeaderFrom(new Path(conf.get(HEADER_FROM_FILE)), conf);
          return super.getRecordWriter(ctx);
      }
  }

  public int run(String[] args) throws Exception {
      final Configuration conf = getConf();

      conf.set(MyOutputFormat.HEADER_FROM_FILE, args[0]);

      final Job job = new Job(conf);

      job.setJarByClass(TestBAM.class);
      job.setMapperClass (TestBAMMapper.class);
      job.setReducerClass(TestBAMReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(SAMRecordWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass (SAMRecordWritable.class);

      job.setInputFormatClass(AnySAMInputFormat.class);
      job.setOutputFormatClass(TestBAM.MyOutputFormat.class);

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
        System.out.printf("Usage: hadoop jar <name.jar> %s <input.bam> <output_directory>\n", TestBAM.class.getCanonicalName());
        System.exit(0);
    }

    int res = ToolRunner.run(new Configuration(), new TestBAM(), args);
    System.exit(res);
  }
}

final class TestBAMMapper
        extends org.apache.hadoop.mapreduce.Mapper<LongWritable,SAMRecordWritable, Text,SAMRecordWritable>
{
    @Override protected void map(
            LongWritable ignored, SAMRecordWritable wrec,
            org.apache.hadoop.mapreduce.Mapper<LongWritable,SAMRecordWritable, Text,SAMRecordWritable>.Context
                    ctx)
            throws InterruptedException, IOException
    {
        final SAMRecord record = wrec.get();
        System.out.println(record.toString());
        ctx.write(new Text(wrec.get().getReadName()), wrec);
    }
}

final class TestBAMReducer
        extends org.apache.hadoop.mapreduce.Reducer<Text,SAMRecordWritable, Text,SAMRecordWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<SAMRecordWritable> records,
            org.apache.hadoop.mapreduce.Reducer<Text, SAMRecordWritable, Text, SAMRecordWritable>.Context ctx)
            throws IOException, InterruptedException {

        final Iterator<SAMRecordWritable> it = records.iterator();

        while (it.hasNext()) {
            SAMRecordWritable a = it.next();
            System.out.println("writing; " + a.get().toString());
            ctx.write(key, a);
        }
    }
}

