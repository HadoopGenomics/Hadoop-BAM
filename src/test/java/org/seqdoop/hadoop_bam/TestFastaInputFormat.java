package org.seqdoop.hadoop_bam;

import hbparquet.hadoop.util.ContextUtil;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestFastaInputFormat {
  private String input;
  private TaskAttemptContext taskAttemptContext;
  private JobContext jobContext;

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    input = ClassLoader.getSystemClassLoader().getResource("auxf.fa").getFile();
    conf.set("mapred.input.dir", "file://" + input);

    taskAttemptContext = ContextUtil.newTaskAttemptContext(conf, mock(TaskAttemptID.class));
    jobContext = ContextUtil.newJobContext(conf, taskAttemptContext.getJobID());
  }

  @Test
  public void testReader() throws Exception {
    FastaInputFormat inputFormat = new FastaInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(1, splits.size());
    RecordReader<Text, ReferenceFragment> reader = inputFormat
        .createRecordReader(splits.get(0), taskAttemptContext);
    reader.initialize(splits.get(0), taskAttemptContext);

    assertTrue(reader.nextKeyValue());
    assertEquals(reader.getCurrentKey(), new Text("Sheila1"));
    ReferenceFragment fragment = reader.getCurrentValue();
    assertEquals(fragment.getSequence(), new Text("GCTAGCTCAGAAAAAAAAAA"));
    reader.close();
  }

}
