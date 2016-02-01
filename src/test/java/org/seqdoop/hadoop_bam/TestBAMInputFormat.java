package org.seqdoop.hadoop_bam;

import hbparquet.hadoop.util.ContextUtil;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordSetBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestBAMInputFormat {
  private String input;
  private TaskAttemptContext taskAttemptContext;
  private JobContext jobContext;

  @Before
  public void setup() throws Exception {
    input = writeNameSortedBamFile().getAbsolutePath();
  }

  private File writeNameSortedBamFile() throws IOException {
    SAMRecordSetBuilder samRecordSetBuilder =
        new SAMRecordSetBuilder(true, SAMFileHeader.SortOrder.queryname);
    for (int i = 0; i < 1000; i++) {
      int chr = 20;
      int start = (i + 1) * 1000;
      int end = start + 100;
      samRecordSetBuilder.addPair(String.format("test-read-%03d", i), chr, start, end);
    }

    final File bamFile = File.createTempFile("test", ".bam");
    bamFile.deleteOnExit();
    SAMFileHeader samHeader = samRecordSetBuilder.getHeader();
    final SAMFileWriter bamWriter = new SAMFileWriterFactory()
        .makeSAMOrBAMWriter(samHeader, true, bamFile);
    for (final SAMRecord rec : samRecordSetBuilder.getRecords()) {
      bamWriter.addAlignment(rec);
    }
    bamWriter.close();
    return bamFile;
  }

  private void completeSetup(boolean keepPairedReadsTogether) {
    Configuration conf = new Configuration();
    conf.set("mapred.input.dir", "file://" + input);
    conf.setBoolean(BAMInputFormat.KEEP_PAIRED_READS_TOGETHER_PROPERTY,
        keepPairedReadsTogether);
    taskAttemptContext = ContextUtil.newTaskAttemptContext(conf, mock(TaskAttemptID.class));
    jobContext = ContextUtil.newJobContext(conf, taskAttemptContext.getJobID());
  }

  @Test
  public void testDontKeepPairedReadsTogether() throws Exception {
    completeSetup(false);
    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(2, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    List<SAMRecord> split1Records = getSAMRecordsFromSplit(inputFormat, splits.get(1));
    assertEquals(1629, split0Records.size());
    assertEquals(371, split1Records.size());
    SAMRecord lastRecordOfSplit0 = split0Records.get(split0Records.size() - 1);
    SAMRecord firstRecordOfSplit1 = split1Records.get(0);
    assertEquals(lastRecordOfSplit0.getReadName(), firstRecordOfSplit1.getReadName());
    assertTrue(lastRecordOfSplit0.getFirstOfPairFlag());
    assertTrue(firstRecordOfSplit1.getSecondOfPairFlag());
  }

  @Test
  public void testKeepPairedReadsTogether() throws Exception {
    completeSetup(true);
    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(2, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    List<SAMRecord> split1Records = getSAMRecordsFromSplit(inputFormat, splits.get(1));
    assertEquals(1630, split0Records.size());
    assertEquals(370, split1Records.size());
    SAMRecord lastRecordOfSplit0 = split0Records.get(split0Records.size() - 1);
    SAMRecord firstRecordOfSplit1 = split1Records.get(0);
    assertNotEquals(lastRecordOfSplit0.getReadName(), firstRecordOfSplit1.getReadName());
  }

  private List<SAMRecord> getSAMRecordsFromSplit(BAMInputFormat inputFormat,
      InputSplit split) throws Exception {
    RecordReader<LongWritable, SAMRecordWritable> reader = inputFormat
        .createRecordReader(split, taskAttemptContext);
    reader.initialize(split, taskAttemptContext);

    List<SAMRecord> records = new ArrayList<SAMRecord>();
    while (reader.nextKeyValue()) {
      SAMRecord r = reader.getCurrentValue().get();
      records.add(r);
    }
    return records;
  }
}
