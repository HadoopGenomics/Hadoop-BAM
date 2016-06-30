package org.seqdoop.hadoop_bam.util;

import htsjdk.samtools.util.BlockCompressedInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;

/**
 * An implementation of {@code SplitCompressionInputStream} for BGZF, based on
 * {@code BZip2CompressionInputStream} and {@code CBZip2InputStream} from Hadoop.
 * (BZip2 is the only splittable compression codec in Hadoop.)
 */
class BGZFSplitCompressionInputStream extends SplitCompressionInputStream {
  private static final int END_OF_BLOCK = -2;
  private final BlockCompressedInputStream input;
  private BufferedInputStream bufferedIn;
  private long startingPos = 0L;
  private long processedPosition;

  private enum POS_ADVERTISEMENT_STATE_MACHINE {
    HOLD, ADVERTISE
  };

  POS_ADVERTISEMENT_STATE_MACHINE posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
  long compressedStreamPosition = 0;

  public BGZFSplitCompressionInputStream(InputStream in, long start, long end)
      throws IOException {
    super(in, start, end);
    bufferedIn = new BufferedInputStream(super.in);
    this.startingPos = super.getPos();
    input = new BlockCompressedInputStream(bufferedIn);
    this.updatePos(false);
  }

  @Override
  public int read() throws IOException {
    byte b[] = new byte[1];
    int result = this.read(b, 0, 1);
    return (result < 0) ? result : (b[0] & 0xff);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    // See BZip2CompressionInputStream#read for implementation notes.
    int result;
    result = readWithinBlock(b, off, len);
    if (result == END_OF_BLOCK) {
      this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE;
    }
    if (this.posSM == POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE) {
      result = readWithinBlock(b, off, off + 1);
      // This is the precise time to update compressed stream position
      // to the client of this code.
      this.updatePos(true);
      this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
    }
    return result;
  }

  /**
   * Read up to <code>len</code> bytes from the stream, but no further than the end of the
   * compressed block. If at the end of the block then no bytes will be read and a return
   * value of -2 will be returned; on the next call to read, bytes from the next block
   * will be returned. This is the same contract as CBZip2InputStream in Hadoop.
   * @return int The return value greater than 0 are the bytes read.  A value
   * of -1 means end of stream while -2 represents end of block.
   */
  private int readWithinBlock(byte[] b, int off, int len) throws IOException {
    if (input.endOfBlock()) {
      final int available = input.available(); // this will read the next block, if there is one
      processedPosition = input.getPosition() >> 16;
      if (available == 0) { // end of stream
        return -1;
      }
      return END_OF_BLOCK;
    }

    // return up to end of block (at most)
    int available = input.available();
    return input.read(b, off, Math.min(available, len));
  }

  @Override
  public void resetState() throws IOException {
    // not implemented (only used in sequence files)
  }

  @Override
  public long getPos() throws IOException {
    return this.compressedStreamPosition;
  }

  // See comment in BZip2CompressionInputStream#updatePos
  private void updatePos(boolean shouldAddOn) {
    int addOn = shouldAddOn ? 1 : 0;
    this.compressedStreamPosition = this.startingPos + processedPosition + addOn;
  }

  @Override
  public void close() throws IOException {
    input.close();
  }
}
