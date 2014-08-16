// Copyright (C) 2011-2012 CRS4.
//
// This file is part of Hadoop-BAM.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package org.seqdoop.hadoop_bam;

import org.seqdoop.hadoop_bam.FastqInputFormat.FastqRecordReader;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TestFastqInputFormat
{
	public static final String oneFastq =
		"@ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1\n" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
		"+\n" +
		"###########################################################################################";

	public static final String twoFastq =
		"@ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1\n" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
		"+\n" +
		"###########################################################################################\n" +

		"@ERR020229.10883 HWI-ST168_161:1:1:1796:2044/1\n" +
		"TGAGCAGATGTGCTAAAGCTGCTTCTCCCCTAGGATCATTTGTACCTACCAGACTCAGGGAAAGGGGTGAGAATTGGGCCGTGGGGCAAGG\n" +
		"+\n" +
		"BDDCDBDD?A=?=:=7,7*@A;;53/53.:@>@@4=>@@@=?1?###############################################";

	public static final String illuminaFastq =
		"@EAS139:136:FC706VJ:2:5:1000:12850 1:Y:18:ATCACG\n" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
		"+\n" +
		"##########################################################################################~";

	public static final String illuminaFastqWithPhred64Quality =
		"@EAS139:136:FC706VJ:2:5:1000:12850 1:Y:18:ATCACG\n" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
		"+\n" +
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

	public static final String oneFastqWithoutRead =
		"@ERR020229.10880 HWI-ST168_161:1:1:1373:2042\n" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
		"+\n" +
		"###########################################################################################";

	public static final String fastqWithIdTwice =
		"@ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1\n" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
		"+ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1\n" +
		"###########################################################################################";

	public static final String fastqWithAmpersandQuality =
		"+lousy.id HWI-ST168_161:1:1:1373:2042/1\n" +
		"@##########################################################################################\n" +
		"@ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1\n" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
		"+ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1\n" +
		"###########################################################################################";

	public static final String illuminaFastqNoFlowCellID =
	  "@EAS139:136::2:5:1000:12850 1:Y:18:ATCACG\n" +
	  "TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
	  "+\n" +
	  "###########################################################################################";

	public static final String illuminaFastqNegativeXYPos =
	  "@EAS139:136:FC706VJ:2:5:-1000:-12850 1:Y:18:ATCACG\n" +
	  "TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
	  "+\n" +
	  "###########################################################################################";

	public static final String illuminaFastqNoIndex =
	  "@EAS139:136::2:5:1000:12850 1:Y:18:\n" +
	  "TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
	  "+\n" +
	  "###########################################################################################";

	public static final String twoFastqWithIllumina =
		"@EAS139:136:FC706VJ:2:5:1000:12850 1:Y:18:ATCACG\n" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
		"+\n" +
		"###########################################################################################\n" +

		"@EAS139:136:FC706VJ:2:5:1000:12850 2:N:18:ATCACG\n" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\n" +
		"+\n" +
		"###########################################################################################\n" +

		"@EAS139:136:FC706VJ:2:5:1000:12850 3:N:18:ATCACG\n" +
		"TGAGCAGATGTGCTAAAGCTGCTTCTCCCCTAGGATCATTTGTACCTACCAGACTCAGGGAAAGGGGTGAGAATTGGGCCGTGGGGCAAGG\n" +
		"+\n" +
		"BDDCDBDD?A=?=:=7,7*@A;;53/53.:@>@@4=>@@@=?1?###############################################";

	private JobConf conf;
	private FileSplit split;
	private File tempFastq;
	private File tempGz;

	private Text key;
	private SequencedFragment fragment;

	@Before
	public void setup() throws IOException
	{
		tempFastq = File.createTempFile("test_fastq_input_format", "fastq");
		tempGz = File.createTempFile("test_fastq_input_format", ".gz");
		conf = new JobConf();
		key = new Text();
		fragment = new SequencedFragment();
	}

	@After
	public void tearDown()
	{
		tempFastq.delete();
		tempGz.delete();
		split = null;
	}

	private void writeToTempFastq(String s) throws IOException
	{
		PrintWriter fastqOut = new PrintWriter( new BufferedWriter( new FileWriter(tempFastq) ) );
		fastqOut.write(s);
		fastqOut.close();
	}

	private FastqRecordReader createReaderForOneFastq() throws IOException
	{
		writeToTempFastq(oneFastq);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, oneFastq.length(), null);

		return new FastqRecordReader(conf, split);
	}

	@Test
	public void testReadFromStart() throws IOException
	{
		FastqRecordReader reader = createReaderForOneFastq();

		assertEquals(0, reader.getPos());
		assertEquals(0.0, reader.getProgress(), 0.01);

		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals("ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1", key.toString());
		assertEquals("TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT", fragment.getSequence().toString());
		assertEquals("###########################################################################################", fragment.getQuality().toString());

		assertEquals(oneFastq.length(), reader.getPos());
		assertEquals(1.0, reader.getProgress(), 0.01);

		retval = reader.next(key, fragment);
		assertFalse(retval);
	}

	@Test
	public void testReadStartInMiddle() throws IOException
	{
		writeToTempFastq(twoFastq);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 10, twoFastq.length() - 10, null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);

		assertEquals(oneFastq.length() + 1, reader.getPos()); // The start of the second record. We +1 for the \n that is not in oneFastq
		assertEquals(0.0, reader.getProgress(), 0.01);

		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals("ERR020229.10883 HWI-ST168_161:1:1:1796:2044/1", key.toString());
		assertEquals("TGAGCAGATGTGCTAAAGCTGCTTCTCCCCTAGGATCATTTGTACCTACCAGACTCAGGGAAAGGGGTGAGAATTGGGCCGTGGGGCAAGG", fragment.getSequence().toString());
		assertEquals("BDDCDBDD?A=?=:=7,7*@A;;53/53.:@>@@4=>@@@=?1?###############################################", fragment.getQuality().toString());

		assertEquals(twoFastq.length(), reader.getPos()); // now should be at the end of the data
		assertEquals(1.0, reader.getProgress(), 0.01);

		retval = reader.next(key, fragment);
		assertFalse(retval);
	}

	@Test
	public void testSliceEndsBeforeEndOfFile() throws IOException
	{
		writeToTempFastq(twoFastq);
		// slice ends at position 10--i.e. somewhere in the first record.  The second record should not be read.
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, 10, null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);

		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals("ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1", key.toString());

		assertFalse("FastqRecordReader is reading a record that starts after the end of the slice", reader.next(key, fragment));
	}

	@Test
	public void testGetReadNumFromName() throws IOException
	{
		FastqRecordReader reader = createReaderForOneFastq();
		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals(1, fragment.getRead().intValue());
	}

	@Test
	public void testNameWithoutReadNum() throws IOException
	{
		writeToTempFastq(oneFastqWithoutRead);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, oneFastqWithoutRead.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);
		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertNull("Read is not null", fragment.getRead());
	}

	@Test
	public void testIlluminaMetaInfo() throws IOException
	{
		writeToTempFastq(illuminaFastq);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, illuminaFastq.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);
		boolean found = reader.next(key, fragment);
		assertTrue(found);

		assertEquals("EAS139", fragment.getInstrument());
		assertEquals(136, fragment.getRunNumber().intValue());
		assertEquals("FC706VJ", fragment.getFlowcellId());
		assertEquals(2, fragment.getLane().intValue());
		assertEquals(5, fragment.getTile().intValue());
		assertEquals(1000, fragment.getXpos().intValue());
		assertEquals(12850, fragment.getYpos().intValue());
		assertEquals(1, fragment.getRead().intValue());
		assertEquals(false, fragment.getFilterPassed().booleanValue());
		assertEquals(18, fragment.getControlNumber().intValue());
		assertEquals("ATCACG", fragment.getIndexSequence());
	}

	@Test
	public void testIlluminaMetaInfoNullFC() throws IOException
	{
		writeToTempFastq(illuminaFastqNoFlowCellID);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, illuminaFastqNoFlowCellID.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);
		boolean found = reader.next(key, fragment);
		assertTrue(found);

		assertEquals("EAS139", fragment.getInstrument());
		assertEquals(136, fragment.getRunNumber().intValue());
		assertEquals("", fragment.getFlowcellId());
		assertEquals(2, fragment.getLane().intValue());
		assertEquals(5, fragment.getTile().intValue());
		assertEquals(1000, fragment.getXpos().intValue());
		assertEquals(12850, fragment.getYpos().intValue());
		assertEquals(1, fragment.getRead().intValue());
		assertEquals(false, fragment.getFilterPassed().booleanValue());
		assertEquals(18, fragment.getControlNumber().intValue());
		assertEquals("ATCACG", fragment.getIndexSequence());
	}

	@Test
	public void testIlluminaMetaInfoNegativeXYpos() throws IOException
	{
		writeToTempFastq(illuminaFastqNegativeXYPos);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, illuminaFastqNegativeXYPos.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);
		boolean found = reader.next(key, fragment);
		assertTrue(found);

		assertEquals("EAS139", fragment.getInstrument());
		assertEquals(136, fragment.getRunNumber().intValue());
		assertEquals("FC706VJ", fragment.getFlowcellId());
		assertEquals(2, fragment.getLane().intValue());
		assertEquals(5, fragment.getTile().intValue());
		assertEquals(-1000, fragment.getXpos().intValue());
		assertEquals(-12850, fragment.getYpos().intValue());
		assertEquals(1, fragment.getRead().intValue());
		assertEquals(false, fragment.getFilterPassed().booleanValue());
		assertEquals(18, fragment.getControlNumber().intValue());
		assertEquals("ATCACG", fragment.getIndexSequence());
	}

	@Test
	public void testOneIlluminaThenNot() throws IOException
	{
		writeToTempFastq(illuminaFastq + "\n" + oneFastq);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, illuminaFastq.length() + oneFastq.length() + 1, null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);

		assertTrue(reader.next(key, fragment));
		assertEquals("EAS139", fragment.getInstrument());

		assertTrue(reader.next(key, fragment));
		assertNull(fragment.getInstrument());

		assertFalse(reader.next(key, fragment));
	}

	@Test
	public void testOneNotThenIllumina() throws IOException
	{
		writeToTempFastq(oneFastq + "\n" + illuminaFastq);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, illuminaFastq.length() + oneFastq.length() + 1, null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);

		assertTrue(reader.next(key, fragment));
		assertNull(fragment.getInstrument());

		assertTrue(reader.next(key, fragment));
		assertNull(fragment.getInstrument());

		assertFalse(reader.next(key, fragment));
	}

	@Test
	public void testProgress() throws IOException
	{
		writeToTempFastq(twoFastq);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, twoFastq.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);
		assertEquals(0.0, reader.getProgress(), 0.01);

		reader.next(key, fragment);
		assertEquals(0.5, reader.getProgress(), 0.01);

		reader.next(key, fragment);
		assertEquals(1.0, reader.getProgress(), 0.01);
	}

	@Test
	public void testCreateKey() throws IOException
	{
		FastqRecordReader reader = createReaderForOneFastq();
		assertTrue(reader.createKey() instanceof Text);
	}

	@Test
	public void testCreateValue() throws IOException
	{
		FastqRecordReader reader = createReaderForOneFastq();
		assertTrue(reader.createValue() instanceof SequencedFragment);
	}

	@Test
	public void testClose() throws IOException
	{
		FastqRecordReader reader = createReaderForOneFastq();
		// doesn't really do anything but exercise the code
		reader.close();
	}

	@Test
	public void testReadFastqWithIdTwice() throws IOException
	{
		writeToTempFastq(fastqWithIdTwice);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, fastqWithIdTwice.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);

		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals("ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1", key.toString());
		assertEquals("TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT", fragment.getSequence().toString());
		assertEquals("###########################################################################################", fragment.getQuality().toString());

		retval = reader.next(key, fragment);
		assertFalse(retval);
	}

	@Test
	public void testReadFastqWithAmpersandQuality() throws IOException
	{
		writeToTempFastq(fastqWithAmpersandQuality);
		// split doesn't start at 0, forcing reader to advance looking for first complete record
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 3, fastqWithAmpersandQuality.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);

		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals("ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1", key.toString());
		assertEquals("TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT", fragment.getSequence().toString());
		assertEquals("###########################################################################################", fragment.getQuality().toString());

		retval = reader.next(key, fragment);
		assertFalse(retval);
	}

	@Test
	public void testMakePositionMessage() throws IOException
	{
		writeToTempFastq(fastqWithIdTwice);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, fastqWithIdTwice.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);
		assertNotNull(reader.makePositionMessage());
	}

	@Test
	public void testFastqWithIlluminaEncoding() throws IOException
	{
		conf.set("hbam.fastq-input.base-quality-encoding", "illumina");
		verifyInputQualityConfig();
	}

	@Test
	public void testFastqWithIlluminaEncodingAndGenericInputConfig() throws IOException
	{
		conf.set("hbam.input.base-quality-encoding", "illumina");
		verifyInputQualityConfig();
	}

	private void verifyInputQualityConfig() throws IOException
	{
		writeToTempFastq(illuminaFastqWithPhred64Quality);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, illuminaFastqWithPhred64Quality.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);
		boolean found = reader.next(key, fragment);
		assertTrue(found);
		assertEquals("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC", fragment.getQuality().toString());
	}

	@Test
	public void testGzCompressedInput() throws IOException
	{
		// write gzip-compressed data
		GzipCodec codec = new GzipCodec();
		PrintWriter fastqOut = new PrintWriter( new BufferedOutputStream( codec.createOutputStream( new FileOutputStream(tempGz) ) ) );
		fastqOut.write(twoFastq);
		fastqOut.close();

		// now try to read it
		split = new FileSplit(new Path(tempGz.toURI().toString()), 0, twoFastq.length(), null);
		FastqRecordReader reader = new FastqRecordReader(conf, split);

		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals("ERR020229.10880 HWI-ST168_161:1:1:1373:2042/1", key.toString());
		assertEquals("TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT", fragment.getSequence().toString());

		retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals("ERR020229.10883 HWI-ST168_161:1:1:1796:2044/1", key.toString());
		assertEquals("TGAGCAGATGTGCTAAAGCTGCTTCTCCCCTAGGATCATTTGTACCTACCAGACTCAGGGAAAGGGGTGAGAATTGGGCCGTGGGGCAAGG", fragment.getSequence().toString());
	}

	@Test(expected=RuntimeException.class)
	public void testCompressedSplit() throws IOException
	{
		// write gzip-compressed data
		GzipCodec codec = new GzipCodec();
		PrintWriter fastqOut = new PrintWriter( new BufferedOutputStream( codec.createOutputStream( new FileOutputStream(tempGz) ) ) );
		fastqOut.write(twoFastq);
		fastqOut.close();

		// now try to read it starting from the middle
		split = new FileSplit(new Path(tempGz.toURI().toString()), 10, twoFastq.length(), null);
		FastqRecordReader reader = new FastqRecordReader(conf, split);
	}

	@Test
	public void testIlluminaNoIndex() throws IOException
	{
		writeToTempFastq(illuminaFastqNoIndex);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, illuminaFastqNoIndex.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);
		boolean found = reader.next(key, fragment);
		assertTrue(found);

		// ensure all meta-data was picked up
		assertEquals("EAS139", fragment.getInstrument());
		assertEquals(136, fragment.getRunNumber().intValue());
		// now verify the index
		assertEquals("", fragment.getIndexSequence());
	}

	@Test
	public void testSkipFailedQC() throws IOException
	{
		conf.set("hbam.fastq-input.filter-failed-qc", "true");
		verifySkipFailedQC();
	}

	@Test
	public void testSkipFailedQCGenericConfig() throws IOException
	{
		conf.set("hbam.input.filter-failed-qc", "true");
		verifySkipFailedQC();
	}

	private void verifySkipFailedQC() throws IOException
	{
		writeToTempFastq(twoFastqWithIllumina);
		split = new FileSplit(new Path(tempFastq.toURI().toString()), 0, twoFastqWithIllumina.length(), null);

		FastqRecordReader reader = new FastqRecordReader(conf, split);
		boolean found = reader.next(key, fragment);
		assertTrue(found);
		assertEquals(2, (int)fragment.getRead());

		found = reader.next(key, fragment);
		assertTrue(found);
		assertEquals(3, (int)fragment.getRead());

		found = reader.next(key, fragment);
		assertFalse(found);
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestFastqInputFormat.class.getName());
	}
}
