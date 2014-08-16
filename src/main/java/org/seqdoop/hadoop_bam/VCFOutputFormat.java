// Copyright (c) 2013 Aalto University
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

// File created: 2013-06-26 56:09:25

package org.seqdoop.hadoop_bam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** An abstract {@link org.apache.hadoop.mapreduce.OutputFormat} for VCF and
 * BCF files. Only locks down the value type and stores the output format
 * requested.
 */
public abstract class VCFOutputFormat<K>
	extends FileOutputFormat<K,VariantContextWritable>
{
	/** A string property defining the output format to use. The value is read
	 * directly by {@link VCFFormat#valueOf}.
	 */
	public static final String OUTPUT_VCF_FORMAT_PROPERTY =
		"hadoopbam.vcf.output-format";

	protected VCFFormat format;

	/** Creates a new output format, reading {@link #OUTPUT_VCF_FORMAT_PROPERTY}
	 * from the given <code>Configuration</code>.
	 */
	protected VCFOutputFormat(Configuration conf) {
		final String fmtStr = conf.get(OUTPUT_VCF_FORMAT_PROPERTY);

		format = fmtStr == null ? null : VCFFormat.valueOf(fmtStr);
	}

	/** Creates a new output format for the given VCF format. */
	protected VCFOutputFormat(VCFFormat fmt) {
		if (fmt == null)
			throw new IllegalArgumentException("null VCFFormat");
		format = fmt;
	}
}
