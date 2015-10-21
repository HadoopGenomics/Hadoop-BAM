package org.seqdoop.hadoop_bam;

import htsjdk.samtools.SAMUtils;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

import static org.junit.Assert.assertEquals;

public class TestBAMSplitGuesser {

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    String bam = getClass().getClassLoader().getResource("test.bam").getFile();
    SeekableStream ss = WrapSeekable.openPath(conf, new Path(bam));
    BAMSplitGuesser bamSplitGuesser = new BAMSplitGuesser(ss, conf);
    long startGuess = bamSplitGuesser.guessNextBAMRecordStart(0, 3 * 0xffff + 0xfffe);
    assertEquals(SAMUtils.findVirtualOffsetOfFirstRecordInBam(new File(bam)), startGuess);
  }
}
