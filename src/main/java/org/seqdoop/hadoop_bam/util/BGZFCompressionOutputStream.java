package org.seqdoop.hadoop_bam.util;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

/**
 * An implementation of {@code CompressionOutputStream} for BGZF, using
 * {@link BlockCompressedOutputStream} from htsjdk. Note that unlike
 * {@link BlockCompressedOutputStream}, an empty gzip block file terminator is
 * <i>not</i> written at the end of the stream. This is because in Hadoop, multiple
 * headerless files are often written in parallel, and merged afterwards into a single
 * file, and it's during the merge process the header and terminator are added.
 */
class BGZFCompressionOutputStream extends CompressionOutputStream {

  private BlockCompressedOutputStream output;

  public BGZFCompressionOutputStream(OutputStream out)
      throws IOException {
    super(out);
    this.output = new BlockCompressedOutputStream(out, (File) null);
  }

  public void write(int b) throws IOException {
    output.write(b);
  }

  public void write(byte[] b, int off, int len) throws IOException {
    output.write(b, off, len);
  }

  public void finish() throws IOException {
    output.flush();
  }

  public void resetState() throws IOException {
    output.flush();
    output = new BlockCompressedOutputStream(out, (File) null);
  }

  public void close() throws IOException {
    output.flush(); // don't close as we don't want to write terminator (empty gzip block)
    out.close();
  }
}
