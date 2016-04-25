package org.seqdoop.hadoop_bam;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestSAMInputFormat {
  private String input;
  private TaskAttemptContext taskAttemptContext;
  private JobContext jobContext;

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    input = ClassLoader.getSystemClassLoader().getResource("test.sam").getFile();
    conf.set("mapred.input.dir", "file://" + input);

    taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
    jobContext = new JobContextImpl(conf, taskAttemptContext.getJobID());
  }

  @Test
  public void testReader() throws Exception {
    int expectedCount = 0;
    SamReader samReader = SamReaderFactory.makeDefault().open(new File(input));
    for (SAMRecord r : samReader) {
      expectedCount++;
    }
    samReader.close();

    AnySAMInputFormat inputFormat = new AnySAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(1, splits.size());
    RecordReader<LongWritable, SAMRecordWritable> reader = inputFormat
        .createRecordReader(splits.get(0), taskAttemptContext);
    reader.initialize(splits.get(0), taskAttemptContext);

    int actualCount = 0;
    while (reader.nextKeyValue()) {
      actualCount++;
    }
    reader.close();

    assertEquals(expectedCount, actualCount);
  }

  @Test
  public void testMapReduceJob() throws Exception {
    Configuration conf = new Configuration();

    FileSystem fileSystem = FileSystem.get(conf);
    Path inputPath = new Path(input);
    Path outputPath = fileSystem.makeQualified(new Path("target/out"));
    fileSystem.delete(outputPath, true);

    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, inputPath);
    job.setInputFormatClass(SAMInputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(SAMRecordWritable.class);
    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, outputPath);

    boolean success = job.waitForCompletion(true);
    assertTrue(success);

    List<String> samStrings = new ArrayList<String>();
    SamReader samReader = SamReaderFactory.makeDefault().open(new File(input));
    for (SAMRecord r : samReader) {
      samStrings.add(r.getSAMString().trim());
    }
    samReader.close();

    File outputFile = new File(new File(outputPath.toUri()), "part-m-00000");
    BufferedReader br = new BufferedReader(new FileReader(outputFile));
    String line;
    int index = 0;
    while ((line = br.readLine()) != null) {
      String value = line.substring(line.indexOf("\t") + 1); // ignore key
      assertEquals(samStrings.get(index++), value);
    }
    br.close();
  }
}
