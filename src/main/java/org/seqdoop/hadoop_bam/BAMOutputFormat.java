// Copyright (c) 2010 Aalto University
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

// File created: 2010-08-11 12:17:33

package org.seqdoop.hadoop_bam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Currently this only locks down the value type of the {@link
 * org.apache.hadoop.mapreduce.OutputFormat}: contains no functionality.
 */
public abstract class BAMOutputFormat<K>
	extends FileOutputFormat<K,SAMRecordWritable> {
	/**
	 * If set to <code>true</code>, write <i>.splitting-bai</i> files for every BAM file
	 * (defaults to <code>false</code>).
	 * A splitting BAI file (not to be confused with a regular BAI file) contains an
	 * index of offsets that the BAM file can be read from; they are used by
	 * {@link BAMInputFormat} to construct splits.
	 */
	public static final String WRITE_SPLITTING_BAI =
			"hadoopbam.bam.write-splitting-bai";

	/**
	 * If set to true, use the Intel deflater for compressing DEFLATE compressed streams.
	 * If set, the <a href="https://github.com/Intel-HLS/GKL">GKL library</a> must be
	 * provided on the classpath.
	 */
	public static final String USE_INTEL_DEFLATER_PROPERTY = "hadoopbam.bam.use-intel-deflater";

	static boolean useIntelDeflater(Configuration conf) {
		return conf.getBoolean(USE_INTEL_DEFLATER_PROPERTY, false);
	}
}
