package org.seqdoop.hadoop_bam;

import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestVCFFormat {

  @Test
  public void testInferFromFilePath() throws IOException {
    assertEquals(VCFFormat.VCF, VCFFormat.inferFromFilePath("test.vcf"));
    assertEquals(VCFFormat.VCF, VCFFormat.inferFromFilePath("test.vcf.gz"));
    assertEquals(VCFFormat.VCF, VCFFormat.inferFromFilePath("test.vcf.bgzf.gz"));
    assertEquals(VCFFormat.VCF, VCFFormat.inferFromFilePath("test.vcf.bgz"));
    assertEquals(VCFFormat.BCF, VCFFormat.inferFromFilePath("test.uncompressed.bcf"));
    assertEquals(VCFFormat.BCF, VCFFormat.inferFromFilePath("test.bgzf.bcf"));
    assertNull(VCFFormat.inferFromFilePath("test.sam"));
  }

  @Test
  public void testInferFromData() throws IOException {
    assertEquals(VCFFormat.VCF, VCFFormat.inferFromData(stream("test.vcf")));
    assertEquals(VCFFormat.VCF, VCFFormat.inferFromData(stream("test.vcf.gz")));
    assertEquals(VCFFormat.VCF, VCFFormat.inferFromData(stream("test.vcf.bgzf.gz")));
    assertEquals(VCFFormat.VCF, VCFFormat.inferFromData(stream("test.vcf.bgz")));
    assertEquals(VCFFormat.BCF, VCFFormat.inferFromData(stream("test.uncompressed.bcf")));
    assertEquals(VCFFormat.BCF, VCFFormat.inferFromData(stream("test.bgzf.bcf")));
    assertNull(VCFFormat.inferFromData(stream("test.sam")));
  }

  private InputStream stream(String resource) throws IOException {
    return ClassLoader.getSystemClassLoader().getResource(resource).openStream();
  }
}
