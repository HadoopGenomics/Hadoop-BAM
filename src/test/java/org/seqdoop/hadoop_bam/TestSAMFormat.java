package org.seqdoop.hadoop_bam;

import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestSAMFormat {

  @Test
  public void testInferFromFilePath() throws IOException {
    assertEquals(SAMFormat.SAM, SAMFormat.inferFromFilePath("test.sam"));
    assertEquals(SAMFormat.BAM, SAMFormat.inferFromFilePath("test.bam"));
    assertEquals(SAMFormat.CRAM, SAMFormat.inferFromFilePath("test.cram"));
    assertNull(SAMFormat.inferFromFilePath("test.vcf"));
  }

  @Test
  public void testInferFromData() throws IOException {
    assertEquals(SAMFormat.SAM, SAMFormat.inferFromData(stream("test.sam")));
    assertEquals(SAMFormat.BAM, SAMFormat.inferFromData(stream("test.bam")));
    assertEquals(SAMFormat.CRAM, SAMFormat.inferFromData(stream("test.cram")));
    assertNull( SAMFormat.inferFromData(stream("test.vcf")));
  }

  private InputStream stream(String resource) throws IOException {
    return ClassLoader.getSystemClassLoader().getResource(resource).openStream();
  }
}
