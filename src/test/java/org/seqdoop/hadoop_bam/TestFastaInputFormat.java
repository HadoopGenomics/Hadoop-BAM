package org.seqdoop.hadoop_bam;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestFastaInputFormat {
  private String input;
  private TaskAttemptContext taskAttemptContext;
  private JobContext jobContext;

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    input = ClassLoader.getSystemClassLoader().getResource("mini-chr1-chr2.fasta").getFile();
    conf.set("mapred.input.dir", "file://" + input);

    // Input fasta is 600 bytes, so this gets us 3 FileInputFormat splits.
    conf.set(FileInputFormat.SPLIT_MAXSIZE, "200");

    taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
    jobContext = new JobContextImpl(conf, taskAttemptContext.getJobID());
  }

  @Test
  public void testReader() throws Exception {
    FastaInputFormat inputFormat = new FastaInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(2, splits.size());
    RecordReader<Text, ReferenceFragment> reader = inputFormat
        .createRecordReader(splits.get(0), taskAttemptContext);
    reader.initialize(splits.get(0), taskAttemptContext);

    assertTrue(reader.nextKeyValue());
    assertEquals(new Text("chr1 dna:chromosome chromosome:GRCh37:1:1:249250621:11"), reader.getCurrentKey());
    assertEquals(new Text("TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTA"), reader.getCurrentValue().getSequence());

    assertTrue(reader.nextKeyValue());
    assertEquals(new Text("chr1 dna:chromosome chromosome:GRCh37:1:1:249250621:182"), reader.getCurrentKey());
    assertEquals(new Text("ACCCTAACCCTAACCCTAACCCTAACCCAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCCTAAC"), reader.getCurrentValue().getSequence());

    assertTrue(reader.nextKeyValue());
    assertEquals(new Text("chr1 dna:chromosome chromosome:GRCh37:1:1:249250621:1163"), reader.getCurrentKey());
    assertEquals(new Text("CCTAACCCTAACCCTAACCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCC"), reader.getCurrentValue().getSequence());

    assertTrue(reader.nextKeyValue());
    assertEquals(new Text("chr1 dna:chromosome chromosome:GRCh37:1:1:249250621:1244"), reader.getCurrentKey());
    assertEquals(new Text("TAACCCTAAACCCTAAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCAACCCCAACCCCAACCCCAACCCCAACCC"), reader.getCurrentValue().getSequence());

    assertTrue(reader.nextKeyValue());
    assertEquals(new Text("chr1 dna:chromosome chromosome:GRCh37:1:1:249250621:1325"), reader.getCurrentKey());
    assertEquals(new Text("CAACCCTAACCCCTAACCCTAACCCTAACCCTACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCCC"), reader.getCurrentValue().getSequence());

    assertFalse(reader.nextKeyValue());

    reader = inputFormat.createRecordReader(splits.get(1), taskAttemptContext);
    reader.initialize(splits.get(1), taskAttemptContext);

    assertTrue(reader.nextKeyValue());
    assertEquals(new Text("chr2 dna:chromosome chromosome:GRCh37:2:1:243199373:11"), reader.getCurrentKey());
    assertEquals(new Text("TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTCGCGGTACCCTC"), reader.getCurrentValue().getSequence());

    assertFalse(reader.nextKeyValue());

    reader.close();
  }

}
