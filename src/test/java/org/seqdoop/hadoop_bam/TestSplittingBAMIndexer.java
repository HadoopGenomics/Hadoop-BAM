package org.seqdoop.hadoop_bam;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSplittingBAMIndexer {
  private String input;

  @Before
  public void setup() throws Exception {
    input = ClassLoader.getSystemClassLoader().getResource("test.bam").getFile();
  }

  @Test
  public void testIndexersProduceSameIndexes() throws Exception {
    long bamFileSize = new File(input).length();
    for (int g : new int[] { 2, 10, SplittingBAMIndexer.DEFAULT_GRANULARITY}) {
      SplittingBAMIndex index1 = fromBAMFile(g);
      SplittingBAMIndex index2 = fromSAMRecords(g);
      assertEquals(index1, index2);
      assertEquals(bamFileSize, index1.bamSize());
      assertEquals(bamFileSize, index2.bamSize());
    }
  }

  private SplittingBAMIndex fromBAMFile(int granularity) throws
      IOException {
    Configuration conf = new Configuration();
    conf.set("input", new File(input).toURI().toString());
    conf.setInt("granularity", granularity);

    SplittingBAMIndexer.run(conf);

    File indexFile = new File(input + SplittingBAMIndexer.OUTPUT_FILE_EXTENSION);
    assertTrue(indexFile.exists());

    return new SplittingBAMIndex(indexFile);
  }

  private SplittingBAMIndex fromSAMRecords(int granularity) throws IOException {
    File indexFile = new File(input + SplittingBAMIndexer.OUTPUT_FILE_EXTENSION);
    FileOutputStream out = new FileOutputStream(indexFile);
    SplittingBAMIndexer indexer = new SplittingBAMIndexer(out, granularity);
    SamReader samReader = SamReaderFactory.makeDefault()
        .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS).open(new File(input));
    for (SAMRecord r : samReader) {
      indexer.processAlignment(r);
    }
    indexer.finish(new File(input).length());
    out.close();

    assertTrue(indexFile.exists());

    return new SplittingBAMIndex(indexFile);
  }
}
