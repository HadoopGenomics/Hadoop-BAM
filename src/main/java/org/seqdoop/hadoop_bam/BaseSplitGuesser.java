package org.seqdoop.hadoop_bam;

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.io.IOUtils;

public class BaseSplitGuesser {

  private final static int BGZF_MAGIC_0 = 0x1f;
  private final static int BGZF_MAGIC_1 = 0x8b;
  private final static int BGZF_MAGIC_2 = 0x08;
  private final static int BGZF_MAGIC_3 = 0x04;
  protected final static int BGZF_MAGIC = 0x04088b1f;
  private final static int BGZF_MAGIC_SUB = 0x00024342;

  protected SeekableStream in;
  protected final ByteBuffer buf;

  public BaseSplitGuesser() {
    buf = ByteBuffer.allocate(8);
    buf.order(ByteOrder.LITTLE_ENDIAN);
  }

  protected static class PosSize {
    public int pos;
    public int size;
    public PosSize(int p, int s) { pos = p; size = s; }
  }

  // Gives the compressed size on the side. Returns null if it doesn't find
  // anything.
  protected PosSize guessNextBGZFPos(int p, int end) {
    try {
      while(true) {
          boolean found_block_start = false;
          boolean in_magic = false;
          in.seek(p);
          while(!found_block_start) {
              int n = in.read();
              
              if (n == BGZF_MAGIC_0) {
                  in_magic = true;
              } else if (n == BGZF_MAGIC_3 && in_magic) {
                  found_block_start = true;
              } else if (p >= end) {
                  return null;
              } else if (!((n == BGZF_MAGIC_1 && in_magic) ||
                           (n == BGZF_MAGIC_2 && in_magic))) {
                  in_magic = false;
              }
              p++;
          }
          
          // after the magic number:
          // skip 6 unspecified bytes (MTIME, XFL, OS)
          // XLEN = 6 (little endian, so 0x0600)
          // SI1  = 0x42
          // SI2  = 0x43
          // SLEN = 0x02
          in.seek(p + 6);
          int n = in.read();
          if (0x06 != n) {
              continue;
          }
          n = in.read();
          if (0x00 != n) {
              continue;
          }
          n = in.read();
          if (0x42 != n) {
              continue;
          }
          n = in.read();
          if (0x43 != n) {
              continue;
          }
          n = in.read();
          if (0x02 != n) {
              continue;
          }         
          int blockSize = (in.read() << 8) + in.read();
          
          return new PosSize(p - 4, blockSize);
      }
    } catch (IOException e) {
      return null;
    }
  }
}
