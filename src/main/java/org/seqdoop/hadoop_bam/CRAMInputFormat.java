package org.seqdoop.hadoop_bam;

import htsjdk.samtools.cram.build.CramContainerIterator;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

public class CRAMInputFormat extends FileInputFormat<LongWritable, SAMRecordWritable> {

  public static final String REFERENCE_SOURCE_PATH_PROPERTY =
      "hadoopbam.cram.reference-source-path";

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    return getSplits(super.getSplits(job), job.getConfiguration());
  }

  public List<InputSplit> getSplits(List<InputSplit> splits, Configuration conf)
      throws IOException {
    // update splits to align with CRAM container boundaries
    List<InputSplit> newSplits = new ArrayList<InputSplit>();
    Map<Path, List<Long>> fileToOffsets = new HashMap<Path, List<Long>>();
    for (InputSplit split : splits) {
      FileSplit fileSplit = (FileSplit) split;
      Path path = fileSplit.getPath();
      List<Long> containerOffsets = fileToOffsets.get(path);
      if (containerOffsets == null) {
        containerOffsets = getContainerOffsets(conf, path);
        fileToOffsets.put(path, containerOffsets);
      }
      long newStart = nextContainerOffset(containerOffsets, fileSplit.getStart());
      long newEnd = nextContainerOffset(containerOffsets, fileSplit.getStart() +
          fileSplit.getLength());
      long newLength = newEnd - newStart;
      if (newLength == 0) { // split is wholly within a container
        continue;
      }
      FileSplit newSplit = new FileSplit(fileSplit.getPath(), newStart, newLength,
          fileSplit.getLocations());
      newSplits.add(newSplit);
    }
    return newSplits;
  }

  private static List<Long> getContainerOffsets(Configuration conf, Path cramFile)
      throws IOException {
    SeekableStream seekableStream = WrapSeekable.openPath(conf, cramFile);
    CramContainerIterator cci = new CramContainerIterator(seekableStream);
    List<Long> containerOffsets = new ArrayList<Long>();
    containerOffsets.add(seekableStream.position());
    while (cci.hasNext()) {
      cci.next();
      containerOffsets.add(seekableStream.position());
    }
    containerOffsets.add(seekableStream.length());
    return containerOffsets;
  }

  private static long nextContainerOffset(List<Long> containerOffsets, long position) {
    for (long offset : containerOffsets) {
      if (offset >= position) {
        return offset;
      }
    }
    throw new IllegalStateException("Could not find position " + position + " in " +
        "container offsets: " + containerOffsets);
  }

  @Override
  public RecordReader<LongWritable, SAMRecordWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    RecordReader<LongWritable, SAMRecordWritable> rr = new CRAMRecordReader();
    rr.initialize(split, context);
    return rr;
  }

  @Override
  public boolean isSplitable(JobContext job, Path path) {
    return true;
  }
}
