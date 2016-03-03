package org.seqdoop.hadoop_bam;

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.io.IOUtils;

class BaseSplitGuesser {

  protected final static int BGZF_MAGIC     = 0x04088b1f;
  protected final static int BGZF_MAGIC_SUB = 0x00024342;
  protected final static int BGZF_SUB_SIZE  = 4 + 2;

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
    try { for (;;) {
      for (;;) {
        in.seek(p);
        IOUtils.readFully(in, buf.array(), 0, 4);
        int n = buf.getInt(0);

        if (n == BGZF_MAGIC)
          break;

        // Skip ahead a bit more than 1 byte if you can.
        if (n >>> 8 == BGZF_MAGIC << 8 >>> 8)
          ++p;
        else if (n >>> 16 == BGZF_MAGIC << 16 >>> 16)
          p += 2;
        else
          p += 3;

        if (p >= end)
          return null;
      }
      // Found what looks like a gzip block header: now get XLEN and
      // search for the BGZF subfield.
      final int p0 = p;
      p += 10;
      in.seek(p);
      IOUtils.readFully(in, buf.array(), 0, 2);
      p += 2;
      final int xlen   = getUShort(0);
      final int subEnd = p + xlen;

      while (p < subEnd) {
        IOUtils.readFully(in, buf.array(), 0, 4);

        if (buf.getInt(0) != BGZF_MAGIC_SUB) {
          p += 4 + getUShort(2);
          in.seek(p);
          continue;
        }

        // Found it: this is close enough to a BGZF block, make it
        // our guess.

        // But find out the size before returning. First, grab bsize:
        // we'll need it later.
        IOUtils.readFully(in, buf.array(), 0, 2);
        int bsize = getUShort(0);

        // Then skip the rest of the subfields.
        p += BGZF_SUB_SIZE;
        while (p < subEnd) {
          in.seek(p);
          IOUtils.readFully(in, buf.array(), 0, 4);
          p += 4 + getUShort(2);
        }
        if (p != subEnd) {
          // Cancel our guess because the xlen field didn't match the
          // data.
          break;
        }

        // Now skip past the compressed data and the CRC-32.
        p += bsize - xlen - 19 + 4;
        in.seek(p);
        IOUtils.readFully(in, buf.array(), 0, 4);
        return new PosSize(p0, buf.getInt(0));
      }
      // No luck: look for the next gzip block header. Start right after
      // where we last saw the identifiers, although we could probably
      // safely skip further ahead. (If we find the correct one right
      // now, the previous block contained 0x1f8b0804 bytes of data: that
      // seems... unlikely.)
      p = p0 + 4;

    }} catch (IOException e) {
      return null;
    }
  }

  protected int getUShort(final int idx) {
    return (int)buf.getShort(idx) & 0xffff;
  }
}
