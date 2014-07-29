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

import org.seqdoop.hadoop_bam.util.ConfHelper;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;

public class TestConfHelper
{
	@Test
	public void testParseBooleanValidValues()
	{
		assertTrue(ConfHelper.parseBoolean("true", false));
		assertTrue(ConfHelper.parseBoolean("tRuE", false));
		assertTrue(ConfHelper.parseBoolean("TRUE", false));
		assertTrue(ConfHelper.parseBoolean("t", false));
		assertTrue(ConfHelper.parseBoolean("yes", false));
		assertTrue(ConfHelper.parseBoolean("y", false));
		assertTrue(ConfHelper.parseBoolean("Y", false));
		assertTrue(ConfHelper.parseBoolean("1", false));

		assertFalse(ConfHelper.parseBoolean("false", true));
		assertFalse(ConfHelper.parseBoolean("faLse", true));
		assertFalse(ConfHelper.parseBoolean("FALSE", true));
		assertFalse(ConfHelper.parseBoolean("f", true));
		assertFalse(ConfHelper.parseBoolean("no", true));
		assertFalse(ConfHelper.parseBoolean("n", true));
		assertFalse(ConfHelper.parseBoolean("N", true));
		assertFalse(ConfHelper.parseBoolean("0", true));
	}

	@Test
	public void testParseBooleanNull()
	{
		assertTrue(ConfHelper.parseBoolean(null, true));
		assertFalse(ConfHelper.parseBoolean(null, false));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testParseBooleanInvalidValue()
	{
		ConfHelper.parseBoolean("dodo", true);
	}

	@Test
	public void testParseBooleanFromConfValue()
	{
		final String propName = "my.property";
		Configuration conf = new Configuration();
		conf.set(propName, "t");
		assertTrue(ConfHelper.parseBoolean(conf, propName, false));
	}

	@Test
	public void testParseBooleanFromConfNull()
	{
		Configuration conf = new Configuration();
		assertTrue(ConfHelper.parseBoolean(conf, "my.property", true));
		assertFalse(ConfHelper.parseBoolean(conf, "my.property", false));
	}


	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestConfHelper.class.getName());
	}
}
