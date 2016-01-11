package org.seqdoop.hadoop_bam;

import htsjdk.samtools.*;
import htsjdk.samtools.cram.CRAMException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

import java.io.InputStream;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class TestSAMHeaderReader {
    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @Test
    public void testBAMHeaderReaderNoReference() throws Exception {

        final Configuration conf = new Configuration();

        InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("test.bam");
        final SamReader samReader = SamReaderFactory.makeDefault().open(SamInputResource.of(inputStream));
        int sequenceCount = samReader.getFileHeader().getSequenceDictionary().size();
        samReader.close();

        inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("test.bam");
        SAMFileHeader samHeader = SAMHeaderReader.readSAMHeaderFrom(inputStream, conf);
        inputStream.close();

        assertEquals(samHeader.getSequenceDictionary().size(), sequenceCount);
    }

    @Test
    public void testCRAMHeaderReaderWithReference() throws Exception {
        final Configuration conf = new Configuration();

        final InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("test.cram");
        final URI reference = ClassLoader.getSystemClassLoader().getResource("auxf.fa").toURI();
        conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, reference.toString());

        SAMFileHeader samHeader = SAMHeaderReader.readSAMHeaderFrom(inputStream, conf);
        inputStream.close();

        assertEquals(samHeader.getSequenceDictionary().size(), 1);
    }

    @Test
    public void testCRAMHeaderReaderNoReference() throws Exception {

        thrown.expect(IllegalStateException.class); // htsjdk throws on CRAM file with no reference provided

        final Configuration conf = new Configuration();
        final InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("test.cram");
        SAMFileHeader samHeader = SAMHeaderReader.readSAMHeaderFrom(inputStream, conf);
        inputStream.close();

        assertEquals(samHeader.getSequenceDictionary().size(), 1);
    }

}
