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

// File created: 2013-07-26 13:54:32

package org.seqdoop.hadoop_bam.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;

import htsjdk.samtools.cram.ref.ReferenceSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import org.seqdoop.hadoop_bam.CRAMInputFormat;

public final class SAMHeaderReader {
	/** A String property corresponding to a ValidationStringency
	 * value. If set, the given stringency is used when any part of the
	 * Hadoop-BAM library reads SAM or BAM.
	 */
	public static final String VALIDATION_STRINGENCY_PROPERTY =
		"hadoopbam.samheaderreader.validation-stringency";

	public static SAMFileHeader readSAMHeaderFrom(Path path, Configuration conf)
		throws IOException
	{
		InputStream i = path.getFileSystem(conf).open(path);
		final SAMFileHeader h = readSAMHeaderFrom(i, conf);
		i.close();
		return h;
	}

	/** Does not close the stream. */
	public static SAMFileHeader readSAMHeaderFrom(
		final InputStream in, final Configuration conf)
	{
		final ValidationStringency
			stringency = getValidationStringency(conf);
		SamReaderFactory readerFactory = SamReaderFactory.makeDefault()
				.setOption(SamReaderFactory.Option.EAGERLY_DECODE, false)
				.setUseAsyncIo(false);
		if (stringency != null) {
			readerFactory.validationStringency(stringency);
		}

		final ReferenceSource refSource = getReferenceSource(conf);
		if (null != refSource) {
			readerFactory.referenceSource(refSource);
		}
		return readerFactory.open(SamInputResource.of(in)).getFileHeader();
	}

	public static ValidationStringency getValidationStringency(
		final Configuration conf)
	{
		final String p = conf.get(VALIDATION_STRINGENCY_PROPERTY);
		return p == null ? null : ValidationStringency.valueOf(p);
	}

	public static ReferenceSource getReferenceSource(
			final Configuration conf)
	{
		//TODO: There isn't anything particularly CRAM-specific about reference source or validation
		// stringency other than that a reference source is required for CRAM files. We should move
		// the reference source and validation stringency property names and utility methods out of
		// CRAMInputFormat and SAMHeaderReader and combine them together into a single class for extracting
		// configuration params, but it would break backward compatibility with existing code that
		// is dependent on the CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY.
		final String refSourcePath = conf.get(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY);
		return refSourcePath == null ? null : new ReferenceSource(NIOFileUtil.asPath(refSourcePath));
	}
}
