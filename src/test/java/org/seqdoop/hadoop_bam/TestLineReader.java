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

import org.seqdoop.hadoop_bam.LineReader;

import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class TestLineReader
{
	public static final String input10 = "0123456789";
	public static final String input22 = "0123456789\n0987654321\n";

	private LineReader reader;
	private Text dest = new Text();

	@Test
	public void testReadBufferedLine() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input22.getBytes()), 22);
		reader.readLine(dest);
		assertEquals("0123456789", dest.toString());
	}

	@Test
	public void testSkipOnBufferedLine() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input22.getBytes()), 22);
		long skipped = reader.skip(1);
		assertEquals(1, skipped);
		reader.readLine(dest);
		assertEquals("123456789", dest.toString());
	}

	@Test
	public void testReadBeyondBuffer() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input22.getBytes()), 5);
		reader.readLine(dest);
		assertEquals("0123456789", dest.toString());
	}

	@Test
	public void testSkipBeyondBuffer() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input22.getBytes()), 5);
		long skipped = reader.skip(11);
		assertEquals(11, skipped);
		reader.readLine(dest);
		assertEquals("0987654321", dest.toString());
	}

	@Test
	public void testSkipBeyondInput() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input10.getBytes()), 5);
		long skipped = reader.skip(11);
		assertEquals(10, skipped);

		skipped = reader.skip(11);
		assertEquals(0, skipped);
	}

}
