package org.seqdoop.hadoop_bam.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

/**
 * A Hadoop {@link CompressionCodec} for the
 * <a href="https://samtools.github.io/hts-specs/SAMv1.pdf">BGZF compression format</a>,
 * which reads and writes files with a <code>.bgz</code> suffix. There is no standard
 * suffix for BGZF-compressed files, and in fact <code>.gz</code> is commonly used, in
 * which case {@link BGZFEnhancedGzipCodec} should be used instead of this class.
 * <p>
 * To use BGZFCodec, set it on the configuration object as follows.
 * </p>
 * {@code
 * conf.set("io.compression.codecs", BGZFCodec.class.getCanonicalName())
 * }
 * @see BGZFEnhancedGzipCodec
 */
public class BGZFCodec extends GzipCodec implements SplittableCompressionCodec {

  public static final String DEFAULT_EXTENSION = ".bgz";

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return new BGZFCompressionOutputStream(out);
  }

  // compressors are not used, so ignore/return null

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {
    return createOutputStream(out); // compressors are not used, so ignore
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return null; // compressors are not used, so return null
  }

  @Override
  public Compressor createCompressor() {
    return null; // compressors are not used, so return null
  }

  @Override
  public SplitCompressionInputStream createInputStream(InputStream seekableIn,
      Decompressor decompressor, long start, long end, READ_MODE readMode) throws IOException {
    BGZFSplitGuesser splitGuesser = new BGZFSplitGuesser(seekableIn);
    long adjustedStart = splitGuesser.guessNextBGZFBlockStart(start, end);
    ((Seekable)seekableIn).seek(adjustedStart);
    return new BGZFSplitCompressionInputStream(seekableIn, adjustedStart, end);
  }

  // fall back to GzipCodec for input streams without a start position

  @Override
  public String getDefaultExtension() {
    return DEFAULT_EXTENSION;
  }
}
