// Copyright (c) 2012 Aalto University
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

// File created: 2012-02-02 11:39:46

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileReader;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecord;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMRecordIterator;

/** See {@link BAMRecordReader} for the meaning of the key. */
public class SAMRecordReader
	extends RecordReader<LongWritable,SAMRecordWritable>
{
	private LongWritable key = new LongWritable();
	private SAMRecordWritable record = new SAMRecordWritable();

	private FSDataInputStream input;
	private SAMRecordIterator iterator;
	private long start, end;

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		final FileSplit split = (FileSplit)spl;
		final Configuration conf = ctx.getConfiguration();

		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(conf);

		input = fs.open(file);

		// Create this reader before seeking so that it gets the file header.
		final SAMFileReader reader = new SAMFileReader(input);

		start =         split.getStart();
		end   = start + split.getLength();

		// We need to skip to the start of the next line.
		if (start > 0) {
			// If we're exactly at the start of a line, we don't want to skip the
			// line, so back up into a line break. If we're in the middle of a
			// line this won't make a difference.
			--start;

			input.seek(start);

			// For simplicity, use a LineReader to skip over the first line. And
			// most definitely don't close it, as we want to keep the stream open
			// for the SAMFileReader.
			start += new LineReader(input, conf).readLine(
				new Text(), 0, (int)Math.min(end - start, Integer.MAX_VALUE));
		}
		iterator = reader.iterator();
	}
	@Override public void close() throws IOException { iterator.close(); }

	@Override public float getProgress() throws IOException {
		final long pos = input.getPos();
		if (pos >= end)
			return 1;
		else
			return (float)(pos - start) / (end - start);
	}
	@Override public LongWritable      getCurrentKey  () { return key; }
	@Override public SAMRecordWritable getCurrentValue() { return record; }

	@Override public boolean nextKeyValue() {
		if (!iterator.hasNext())
			return false;

		final SAMRecord r = iterator.next();

		int idx = r.getReferenceIndex();
		if (idx == -1)
			idx = Integer.MAX_VALUE;

		key.set((long)idx << 32 | r.getAlignmentStart() - 1);
		record.set(r);
		return true;
	}
}
