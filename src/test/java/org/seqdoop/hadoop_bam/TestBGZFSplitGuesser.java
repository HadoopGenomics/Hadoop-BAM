package org.seqdoop.hadoop_bam;

import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.seqdoop.hadoop_bam.util.BGZFSplitGuesser;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestBGZFSplitGuesser {

  private final File file;
  private final long firstSplit;
  private final long lastSplit;

  public TestBGZFSplitGuesser(String filename, long firstSplit, long lastSplit) {
    this.file = new File("src/test/resources/" + filename);
    this.firstSplit = firstSplit;
    this.lastSplit = lastSplit;
  }

  @Parameterized.Parameters
  public static Collection<Object> data() {
    return Arrays.asList(new Object[][] {
        {"test.vcf.bgzf.gz", 821, 821}, {"HiSeq.10000.vcf.bgzf.gz", 16688, 509222}
    });
  }

  @Test
  public void test() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(file.toURI());
    FSDataInputStream fsDataInputStream = path.getFileSystem(conf).open(path);
    BGZFSplitGuesser bgzfSplitGuesser = new BGZFSplitGuesser(fsDataInputStream);
    LinkedList<Long> boundaries = new LinkedList<>();
    long start = 1;
    while (true) {
      long end = file.length();
      long nextStart = bgzfSplitGuesser.guessNextBGZFBlockStart(start, end);
      if (nextStart == end) {
        break;
      }
      boundaries.add(nextStart);
      canReadFromBlockStart(nextStart);
      start = nextStart + 1;
    }
    assertEquals(firstSplit, (long) boundaries.getFirst());
    assertEquals(lastSplit, (long) boundaries.getLast());

    assertEquals("Last block start is terminator gzip block",
        file.length() - BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK.length,
        (long) boundaries.get(boundaries.size() - 1));
  }

  private void canReadFromBlockStart(long blockStart) throws IOException {
    BlockCompressedInputStream blockCompressedInputStream = new
        BlockCompressedInputStream(file);
    blockCompressedInputStream.setCheckCrcs(true);
    blockCompressedInputStream.seek(blockStart << 16);
    byte[] b = new byte[100];
    blockCompressedInputStream.read(b);
  }
}
