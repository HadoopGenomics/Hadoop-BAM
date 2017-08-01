package org.seqdoop.hadoop_bam;

import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.Interval;
import java.io.File;
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
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestBAMInputFormat {
  private String input;
  private TaskAttemptContext taskAttemptContext;
  private JobContext jobContext;

  private void completeSetup() {
    completeSetup(false, null, true);
  }

  private void completeSetupWithIntervals(List<Interval> intervals) {
    completeSetupWithBoundedTraversal(intervals, false);
  }

  private void completeSetupWithBoundedTraversal(List<Interval> intervals, boolean
      traverseUnplacedUnmapped) {
    completeSetup(true, intervals, traverseUnplacedUnmapped);
  }

  private void completeSetup(boolean boundedTraversal, List<Interval> intervals, boolean
      traverseUnplacedUnmapped) {
    Configuration conf = new Configuration();
    conf.set("mapred.input.dir", "file://" + input);
    if (boundedTraversal) {
      BAMInputFormat.setTraversalParameters(conf, intervals, traverseUnplacedUnmapped);
    }
    taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
    jobContext = new JobContextImpl(conf, taskAttemptContext.getJobID());
  }

  @Test
  public void testNoReadsInFirstSplitBug() throws Exception {
    input = BAMTestUtil.writeBamFileWithLargeHeader().getAbsolutePath();
    completeSetup();
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(1, splits.size());
  }

  @Test
  public void testMultipleSplits() throws Exception {
    input = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.queryname)
        .getAbsolutePath();
    completeSetup();
    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(2, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    List<SAMRecord> split1Records = getSAMRecordsFromSplit(inputFormat, splits.get(1));
    assertEquals(1577, split0Records.size());
    assertEquals(425, split1Records.size());
    SAMRecord lastRecordOfSplit0 = split0Records.get(split0Records.size() - 1);
    SAMRecord firstRecordOfSplit1 = split1Records.get(0);
    assertEquals(lastRecordOfSplit0.getReadName(), firstRecordOfSplit1.getReadName());
    assertTrue(lastRecordOfSplit0.getFirstOfPairFlag());
    assertTrue(firstRecordOfSplit1.getSecondOfPairFlag());
  }

  @Test
  public void testMultipleSplitsBaiEnabled() throws Exception {
    input = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate)
        .getAbsolutePath();
    completeSetup();
    BAMInputFormat.setEnableBAISplitCalculator(jobContext.getConfiguration(), true);
    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(3, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    List<SAMRecord> split1Records = getSAMRecordsFromSplit(inputFormat, splits.get(1));
    List<SAMRecord> split2Records = getSAMRecordsFromSplit(inputFormat, splits.get(2));
    assertEquals(1080, split0Records.size());
    assertEquals(524, split1Records.size());
    assertEquals(398, split2Records.size());
  }

  @Test
  public void testMultipleSplitsBaiEnabledSuffixPath() throws Exception {
    input = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate)
        .getAbsolutePath();
    File index = new File(input.replaceFirst("\\.bam$", BAMIndex.BAMIndexSuffix));
    index.renameTo(new File(input + BAMIndex.BAMIndexSuffix));
    completeSetup();
    BAMInputFormat.setEnableBAISplitCalculator(jobContext.getConfiguration(), true);
    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(3, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    List<SAMRecord> split1Records = getSAMRecordsFromSplit(inputFormat, splits.get(1));
    List<SAMRecord> split2Records = getSAMRecordsFromSplit(inputFormat, splits.get(2));
    assertEquals(1080, split0Records.size());
    assertEquals(524, split1Records.size());
    assertEquals(398, split2Records.size());
  }

  @Test
  public void testMultipleSplitsBaiEnabledNoIndex() throws Exception {
    input = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.queryname)
        .getAbsolutePath();
    completeSetup();
    BAMInputFormat.setEnableBAISplitCalculator(jobContext.getConfiguration(), true);
    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(2, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    List<SAMRecord> split1Records = getSAMRecordsFromSplit(inputFormat, splits.get(1));
    assertEquals(1577, split0Records.size());
    assertEquals(425, split1Records.size());
    SAMRecord lastRecordOfSplit0 = split0Records.get(split0Records.size() - 1);
    SAMRecord firstRecordOfSplit1 = split1Records.get(0);
    assertEquals(lastRecordOfSplit0.getReadName(), firstRecordOfSplit1.getReadName());
    assertTrue(lastRecordOfSplit0.getFirstOfPairFlag());
    assertTrue(firstRecordOfSplit1.getSecondOfPairFlag());
  }

  @Test
  public void testIntervals() throws Exception {
    input = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate)
        .getAbsolutePath();
    List<Interval> intervals = new ArrayList<Interval>();
    intervals.add(new Interval("chr21", 5000, 9999)); // includes two unpaired fragments
    intervals.add(new Interval("chr21", 20000, 22999));

    completeSetupWithIntervals(intervals);

    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(1, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    assertEquals(16, split0Records.size());
  }

  @Test
  public void testIntervalCoveringWholeChromosome() throws Exception {
    input = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate)
        .getAbsolutePath();
    List<Interval> intervals = new ArrayList<Interval>();
    intervals.add(new Interval("chr21", 1, 1000135));

    completeSetupWithIntervals(intervals);

    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(2, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    List<SAMRecord> split1Records = getSAMRecordsFromSplit(inputFormat, splits.get(1));
    assertEquals(1577, split0Records.size());
    assertEquals(423, split1Records.size());
  }

  @Test
  public void testIntervalsAndUnmapped() throws Exception {
    input = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate)
        .getAbsolutePath();
    List<Interval> intervals = new ArrayList<Interval>();
    intervals.add(new Interval("chr21", 5000, 9999)); // includes two unpaired fragments
    intervals.add(new Interval("chr21", 20000, 22999));

    completeSetupWithBoundedTraversal(intervals, true);

    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(2, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    List<SAMRecord> split1Records = getSAMRecordsFromSplit(inputFormat, splits.get(1));
    assertEquals(16, split0Records.size());
    assertEquals(2, split1Records.size());
  }

  @Test
  public void testUnmapped() throws Exception {
    input = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate)
        .getAbsolutePath();

    completeSetupWithBoundedTraversal(null, true);

    jobContext.getConfiguration().setInt(FileInputFormat.SPLIT_MAXSIZE, 40000);
    BAMInputFormat inputFormat = new BAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(1, splits.size());
    List<SAMRecord> split0Records = getSAMRecordsFromSplit(inputFormat, splits.get(0));
    assertEquals(2, split0Records.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMappedOnly() throws Exception {
    input = BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate)
        .getAbsolutePath();

    // Mapped only (-XL unmapped) is currently unsupported and throws an exception.
    completeSetupWithBoundedTraversal(null, false);
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
