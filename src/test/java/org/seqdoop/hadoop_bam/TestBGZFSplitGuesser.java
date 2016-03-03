package org.seqdoop.hadoop_bam;

import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.seqdoop.hadoop_bam.util.BGZFSplitGuesser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBGZFSplitGuesser {
  @Test
  public void test() throws Exception {
    File bgzf = File.createTempFile("TestBGZFSplitGuesser", ".bgzf");
    BlockCompressedOutputStream bgzfOut = new BlockCompressedOutputStream(bgzf);
    String chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    StringBuilder data = new StringBuilder();
    Random r = new Random();
    r.setSeed(100);
    for (int i = 0; i < 1024 * 150; i++) {
      char rc = chars.charAt(r.nextInt(chars.length()));
      bgzfOut.write(rc);
      data.append(rc);
    }
    bgzfOut.close();

    Configuration conf = new Configuration();
    Path path = new Path(bgzf.toURI());
    FSDataInputStream fsDataInputStream = path.getFileSystem(conf).open(path);
    BGZFSplitGuesser bgzfSplitGuesser = new BGZFSplitGuesser(fsDataInputStream);
    List<Long> boundaries = new ArrayList<>();
    long start = 1;
    while (true) {
      long end = bgzf.length();
      long nextStart = bgzfSplitGuesser.guessNextBGZFBlockStart(start, end);
      if (nextStart == end) {
        break;
      }
      boundaries.add(nextStart);
      start = nextStart + 1;
    }

    assertEquals(3, boundaries.size());

    for (int i = 0; i < boundaries.size() - 1; i++) {
      long boundary = boundaries.get(i);
      BlockCompressedInputStream blockCompressedInputStream = new
          BlockCompressedInputStream(bgzf);
      blockCompressedInputStream.setCheckCrcs(true);
      blockCompressedInputStream.seek(boundary << 16);
      byte[] b = new byte[5]; // check 5 bytes after boundary
      blockCompressedInputStream.read(b);
      assertTrue(data.toString().contains(new String(b)));
    }

    assertEquals("Last block start is terminator gzip block",
        bgzf.length() - BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK.length,
        (long) boundaries.get(boundaries.size() - 1));
  }
}
