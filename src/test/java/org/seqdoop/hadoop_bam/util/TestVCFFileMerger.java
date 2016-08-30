package org.seqdoop.hadoop_bam.util;

import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;

public class TestVCFFileMerger {

  private String partsDirectory;
  private VCFHeader header;

  @Before
  public void setup() throws Exception {
    File partsDir = File.createTempFile("parts", "");
    partsDir.delete();
    partsDir.mkdir();
    Files.createFile(new File(partsDir, "_SUCCESS").toPath());
    partsDirectory = partsDir.toURI().toString();
    header = VCFHeaderReader.readHeaderFrom(TestVCFHeaderReader.seekableStream("test.vcf"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmpty() throws IOException {
    File out = File.createTempFile("out", ".vcf");
    out.deleteOnExit();
    VCFFileMerger.mergeParts(partsDirectory, out.toURI().toString(), header);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBCFNotSupported() throws IOException {
    File out = File.createTempFile("out", ".bcf");
    out.deleteOnExit();
    Path target = Paths.get(URI.create(partsDirectory)).resolve("part-m-00000");
    Files.copy(stream("test.uncompressed.bcf"), target);
    VCFFileMerger.mergeParts(partsDirectory, out.toURI().toString(), header);
  }

  private InputStream stream(String resource) throws IOException {
    return ClassLoader.getSystemClassLoader().getResource(resource).openStream();
  }
}
