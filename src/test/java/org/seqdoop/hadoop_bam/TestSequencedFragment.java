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

import org.junit.*;
import static org.junit.Assert.*;

import org.seqdoop.hadoop_bam.SequencedFragment;
import org.seqdoop.hadoop_bam.FormatConstants;
import org.seqdoop.hadoop_bam.FormatException;

import java.io.IOException;
import java.io.DataInput;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import java.io.DataOutput;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.Text;

public class TestSequencedFragment
{
	private SequencedFragment frag;
	private SequencedFragment frag2;

	@Before
	public void setup()
	{
		frag = new SequencedFragment();
		frag2 = new SequencedFragment();
	}

	@Test
	public void testInitialState()
	{
		assertNotNull(frag.getSequence());
		assertNotNull(frag.getQuality());

		assertNull(frag.getInstrument());
		assertNull(frag.getRunNumber());
		assertNull(frag.getFlowcellId());
		assertNull(frag.getLane());
		assertNull(frag.getTile());
		assertNull(frag.getXpos());
		assertNull(frag.getYpos());
		assertNull(frag.getRead());
		assertNull(frag.getFilterPassed());
		assertNull(frag.getControlNumber());
		assertNull(frag.getIndexSequence());

		assertNotNull(frag.toString());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNoNullSequence()
	{
		frag.setSequence(null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNoNullQuality()
	{
		frag.setQuality(null);
	}

	///////////////////////////////////////////////////////////////
	// equals
	///////////////////////////////////////////////////////////////
	@Test
	public void testEquals()
	{
		assertTrue(frag.equals(frag2));

		frag.getSequence().append("AAAA".getBytes(), 0, 4);
		assertFalse( frag.equals(frag2) );
	}

	@Test
	public void testEqualsSequence()
	{
		frag.getSequence().append("AAAA".getBytes(), 0, 4);
		assertFalse( frag.equals(frag2) );
		frag2.getSequence().append("AAAA".getBytes(), 0, 4);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsQuality()
	{
		frag.getQuality().append("AAAA".getBytes(), 0, 4);
		assertFalse( frag.equals(frag2) );
		frag2.getQuality().append("AAAA".getBytes(), 0, 4);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsInstrument()
	{
		frag.setInstrument("instrument");
		assertFalse( frag.equals(frag2) );
		frag2.setInstrument("instrument");
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsRunNumber()
	{
		frag.setRunNumber(240);
		assertFalse( frag.equals(frag2) );
		frag2.setRunNumber(240);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsFlowcellId()
	{
		frag.setFlowcellId("id");
		assertFalse( frag.equals(frag2) );
		frag2.setFlowcellId("id");
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsLane()
	{
		frag.setLane(2);
		assertFalse( frag.equals(frag2) );
		frag2.setLane(2);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsTile()
	{
		frag.setTile(1000);
		assertFalse( frag.equals(frag2) );
		frag2.setTile(1000);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsXpos()
	{
		frag.setXpos(1234);
		assertFalse( frag.equals(frag2) );
		frag2.setXpos(1234);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsYpos()
	{
		frag.setYpos(1234);
		assertFalse( frag.equals(frag2) );
		frag2.setYpos(1234);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsRead()
	{
		frag.setRead(2);
		assertFalse( frag.equals(frag2) );
		frag2.setRead(2);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsFilterPassed()
	{
		frag.setFilterPassed(false);
		assertFalse( frag.equals(frag2) );
		frag2.setFilterPassed(false);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsControlNumber()
	{
		frag.setControlNumber(314);
		assertFalse( frag.equals(frag2) );
		frag2.setControlNumber(314);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsIndexSequence()
	{
		frag.setIndexSequence("ABC");
		assertFalse( frag.equals(frag2) );
		frag2.setIndexSequence("ABC");
		assertTrue( frag.equals(frag2) );
	}

	///////////////////////////////////////////////////////////////
	// serialization
	///////////////////////////////////////////////////////////////
	private static SequencedFragment cloneBySerialization(SequencedFragment original) throws IOException
	{
		ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
		DataOutputStream dataOutput = new DataOutputStream(outputBuffer);
		original.write(dataOutput);
		dataOutput.close();

		SequencedFragment newFrag = new SequencedFragment();
		newFrag.readFields( new DataInputStream( new ByteArrayInputStream(outputBuffer.toByteArray())));

		return newFrag;
	}

	@Test
	public void testSerializationEmpty() throws IOException
	{
		assertEquals(frag, cloneBySerialization(frag));
	}

	@Test
	public void testSerializationWithSeq() throws IOException
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("##############################"));
		assertEquals(frag, cloneBySerialization(frag));
	}

	@Test
	public void testSerializationWithFields() throws IOException
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"));

		frag.setInstrument("machine");
		frag.setLane(3);
		frag.setRead(1);
		frag.setIndexSequence("CAT");

		assertEquals(frag, cloneBySerialization(frag));
	}

	@Test
	public void testToString()
	{
		String seq = "AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT";
		String qual = "##############################";
		frag.setSequence(new Text(seq));
		frag.setQuality(new Text(qual));

		frag.setInstrument("machine");
		frag.setRunNumber(123);
		frag.setFlowcellId("flowcell");
		frag.setLane(3);
		frag.setTile(1001);
		frag.setXpos(1234);
		frag.setYpos(4321);
		frag.setIndexSequence("CAT");
		frag.setRead(1);

		assertEquals("machine\t123\tflowcell\t3\t1001\t1234\t4321\tCAT\t1\t" + seq + "\t" + qual + "\t1", frag.toString());
	}

	@Test
	public void testVerifyQualitySangerOk()
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("##############################"));
		assertEquals(-1, SequencedFragment.verifyQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Sanger));
	}

	@Test
	public void testVerifyQualityIlluminaOk()
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"));
		assertEquals(-1, SequencedFragment.verifyQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Illumina));
	}

	@Test
	public void testVerifyQualitySangerOutOfRange()
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("#############################" + Character.toString((char)127))); // over range
		assertEquals(29, SequencedFragment.verifyQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Sanger));

		frag.setQuality(new Text("##### ########################")); // under range
		assertEquals(5, SequencedFragment.verifyQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Sanger));
	}

	@Test
	public void testVerifyQualityIlluminaOutOfRange()
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("zzz=zzzzzzzzzzzzzzzzzzzzzzzzzz"));
		assertEquals(3, SequencedFragment.verifyQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Illumina));
	}

	@Test
	public void testConvertQualityIlluminaToSanger()
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"));
		SequencedFragment.convertQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Illumina, FormatConstants.BaseQualityEncoding.Sanger);
		assertEquals("[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[", frag.getQuality().toString());
	}

	@Test
	public void testConvertQualitySangerToIllumina()
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["));
		SequencedFragment.convertQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Sanger, FormatConstants.BaseQualityEncoding.Illumina);
		assertEquals("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", frag.getQuality().toString());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testConvertQualityNoop()
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["));
		SequencedFragment.convertQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Sanger, FormatConstants.BaseQualityEncoding.Sanger);
	}

	@Test(expected=FormatException.class)
	public void testConvertQualityIlluminaOutOfRange()
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("zzz=zzzzzzzzzzzzzzzzzzzzzzzzzz"));
		SequencedFragment.convertQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Illumina, FormatConstants.BaseQualityEncoding.Sanger);
	}

	@Test(expected=FormatException.class)
	public void testConvertQualitySangerUnderRange()
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("### ##########################"));
		SequencedFragment.convertQuality(frag.getQuality(), FormatConstants.BaseQualityEncoding.Sanger, FormatConstants.BaseQualityEncoding.Illumina);
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestSequencedFragment.class.getName());
	}
}
