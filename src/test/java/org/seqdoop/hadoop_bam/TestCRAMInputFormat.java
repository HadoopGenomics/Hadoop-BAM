package org.seqdoop.hadoop_bam;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestCRAMInputFormat {
  private String input;
  private String reference;
  private TaskAttemptContext taskAttemptContext;
  private JobContext jobContext;

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    input = ClassLoader.getSystemClassLoader().getResource("test.cram").getFile();
    reference = ClassLoader.getSystemClassLoader().getResource("auxf.fa").toURI().toString();
    conf.set("mapred.input.dir", "file://" + input);
    conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, reference);

    taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
    jobContext = new JobContextImpl(conf, taskAttemptContext.getJobID());
  }

  @Test
  public void testReader() throws Exception {
    int expectedCount = 0;
    SamReader samReader = SamReaderFactory.makeDefault()
        .referenceSequence(new File(URI.create(reference))).open(new File(input));
    for (SAMRecord r : samReader) {
      expectedCount++;
    }

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

    assertEquals(expectedCount, actualCount);
  }

  @Test
  public void testGetSplits_SpanningContainerBoundary() throws IOException {
    // original splits = 0+1000, 1000+1000, 2000+1000, 3000+433
    checkSplits(1000);
  }

  @Test
  public void testGetSplits_OnContainerBoundary() throws IOException {
    // original splits = 0+83, 83+83, 166+83, ...,  3320+83 (=3403), 3403+30
    checkSplits(83);
  }

  @Test
  public void testGetSplits_OneBeforeContainerBoundary() throws IOException {
    // original splits = 0+534, 534+534 (=1068), ...
    checkSplits(534);
  }

  @Test
  public void testGetSplits_OneAfterContainerBoundary() throws IOException {
    // original splits = 0+535, 535+535 (=1070), ...
    checkSplits(535);
  }

  private void checkSplits(int splitMaxSize) throws IOException {
    // test.cram has containers at positions 1069 and 3403. The file length is 3433.
    // expected splits = 1069+2334, 3403+30
    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, splitMaxSize);
    CRAMInputFormat inputFormat = new CRAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(2, splits.size());
    FileSplit split0 = (FileSplit) splits.get(0);
    FileSplit split1 = (FileSplit) splits.get(1);
    assertEquals(1069, split0.getStart());
    assertEquals(2334, split0.getLength());
    assertEquals(3403, split1.getStart());
    assertEquals(30, split1.getLength());
  }

  @Test
  public void testMapReduceJob() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, reference);

    FileSystem fileSystem = FileSystem.get(conf);
    Path inputPath = new Path(input);
    Path outputPath = fileSystem.makeQualified(new Path("target/out"));
    fileSystem.delete(outputPath, true);

    Job job = Job.getInstance(conf);
    FileInputFormat.setInputPaths(job, inputPath);
    job.setInputFormatClass(CRAMInputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(SAMRecordWritable.class);
    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, outputPath);

    boolean success = job.waitForCompletion(true);
    assertTrue(success);

    List<String> samStrings = new ArrayList<String>();
    SamReader samReader = SamReaderFactory.makeDefault()
        .referenceSequence(new File(URI.create(reference))).open(new File(input));
    for (SAMRecord r : samReader) {
      samStrings.add(r.getSAMString().trim());
    }

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
