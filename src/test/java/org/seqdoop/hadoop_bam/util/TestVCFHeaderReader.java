package org.seqdoop.hadoop_bam.util;

import static org.junit.Assert.assertNotNull;

import com.google.common.io.Resources;
import htsjdk.samtools.seekablestream.ByteArraySeekableStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import org.junit.Test;

public class TestVCFHeaderReader {

  static SeekableStream seekableStream(final String resource) throws IOException {
    return new ByteArraySeekableStream(
        Resources.toByteArray(ClassLoader.getSystemClassLoader().getResource(resource)));
  }

  @Test
  public void testReadHeaderFromVCF() throws IOException {
    assertNotNull(VCFHeaderReader.readHeaderFrom(seekableStream("test.vcf")));
  }

  @Test
  public void testReadHeaderFromGzippedVCF() throws IOException {
    assertNotNull(VCFHeaderReader.readHeaderFrom(seekableStream("test.vcf.gz")));
  }

  @Test
  public void testReadHeaderFromBGZFVCF() throws IOException {
    assertNotNull(VCFHeaderReader.readHeaderFrom(seekableStream("test.vcf.bgzf.gz")));
  }
}
