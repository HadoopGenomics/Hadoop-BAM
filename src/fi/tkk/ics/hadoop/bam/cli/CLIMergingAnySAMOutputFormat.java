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

// File created: 2013-06-25 16:24:52

package fi.tkk.ics.hadoop.bam.cli;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import fi.tkk.ics.hadoop.bam.KeyIgnoringAnySAMOutputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

// Like a KeyIgnoringAnySAMOutputFormat<K>, but sets the SAMFileHeader to
// Utils.getSAMHeaderMerger().getMergedHeader() and allows the output directory
// (the "work directory") to exist.
public class CLIMergingAnySAMOutputFormat<K>
	extends FileOutputFormat<K, SAMRecordWritable>
{
	private KeyIgnoringAnySAMOutputFormat<K> baseOF;

	private void initBaseOF(Configuration conf) {
		if (baseOF == null)
			baseOF = new KeyIgnoringAnySAMOutputFormat<K>(conf);
	}

	@Override public RecordWriter<K,SAMRecordWritable> getRecordWriter(
			TaskAttemptContext context)
		throws IOException
	{
		initBaseOF(context.getConfiguration());

		if (baseOF.getSAMHeader() == null)
			baseOF.setSAMHeader(Utils.getSAMHeaderMerger(
				context.getConfiguration()).getMergedHeader());

		return baseOF.getRecordWriter(context, getDefaultWorkFile(context, ""));
	}

	@Override public Path getDefaultWorkFile(TaskAttemptContext ctx, String ext)
		throws IOException
	{
		initBaseOF(ctx.getConfiguration());
		return Utils.getMergeableWorkFile(
			baseOF.getDefaultWorkFile(ctx, ext).getParent(), "", "", ctx, ext);
	}

	// Allow the output directory to exist.
	@Override public void checkOutputSpecs(JobContext job) {}
}
