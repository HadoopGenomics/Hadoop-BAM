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

import org.seqdoop.hadoop_bam.QseqOutputFormat.QseqRecordWriter;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class TestQseqOutputFormat
{
	private SequencedFragment fragment;

	private ByteArrayOutputStream outputBuffer;
	private DataOutputStream dataOutput;
	private QseqRecordWriter writer;

	@Before
	public void setup() throws IOException
	{
		fragment = new SequencedFragment();
		fragment.setInstrument("instrument");
		fragment.setRunNumber(1);
		fragment.setFlowcellId("xyz");
		fragment.setLane(2);
		fragment.setTile(1001);
		fragment.setXpos(10000);
		fragment.setYpos(9999);
		fragment.setRead(1);
		fragment.setFilterPassed(true);
		fragment.setIndexSequence("CATCAT");
		fragment.setSequence(new Text("AAAAAAAAAA"));
		fragment.setQuality(new Text("##########"));

		outputBuffer = new ByteArrayOutputStream();
		dataOutput = new DataOutputStream(outputBuffer);
		writer = new QseqRecordWriter(new Configuration(), dataOutput);
	}

	@Test
	public void testSimple() throws IOException
	{
		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(11, fields.length);

		assertEquals(fragment.getInstrument(), fields[0]);
		assertEquals(fragment.getRunNumber().toString(), fields[1]);
		assertEquals(fragment.getLane().toString(), fields[2]);
		assertEquals(fragment.getTile().toString(), fields[3]);
		assertEquals(fragment.getXpos().toString(), fields[4]);
		assertEquals(fragment.getYpos().toString(), fields[5]);
		assertEquals(fragment.getIndexSequence().toString(), fields[6]);
		assertEquals(fragment.getRead().toString(), fields[7]);
		assertEquals(fragment.getSequence().toString(), fields[8]);
		assertEquals(fragment.getQuality().toString().replace('#', 'B'), fields[9]);
		assertEquals(fragment.getFilterPassed() ? "1\n" : "0\n", fields[10]);
	}

	@Test
	public void testConvertUnknowns() throws IOException, UnsupportedEncodingException
	{
		String seq = "AAAAANNNNN";
		fragment.setSequence(new Text(seq));
		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(seq.replace("N", "."), fields[8]);
	}

	@Test
	public void testConvertUnknownsInIndexSequence() throws IOException, UnsupportedEncodingException
	{
		String index = "CATNNN";
		fragment.setIndexSequence(index);
		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(index.replace("N", "."), fields[6]);
	}

	@Test
	public void testBaseQualities() throws IOException
	{
		// ensure sanger qualities are converted to illumina
		String seq = "AAAAAAAAAA";
		String qual = "##########";

		fragment.setSequence(new Text(seq));
		fragment.setQuality(new Text(qual));

		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(qual.replace("#", "B"), fields[9]);
	}

	@Test
	public void testConfigureOutputInSanger() throws IOException
	{
		String seq = "AAAAAAAAAA";
		String qual = "##########";

		fragment.setSequence(new Text(seq));
		fragment.setQuality(new Text(qual));

		Configuration conf = new Configuration();
		conf.set("hbam.qseq-output.base-quality-encoding", "sanger");
		writer.setConf(conf);

		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(qual, fields[9]);
	}

	@Test
	public void testClose() throws IOException
	{
		// doesn't really do anything but exercise the code
		writer.close(null);
	}

	@Test
	public void testNoIndex() throws IOException
	{
		fragment.setIndexSequence(null);
		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(11, fields.length);

		assertEquals("0", fields[6]);
	}
}
