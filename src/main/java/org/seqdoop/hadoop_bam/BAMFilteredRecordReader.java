// Copyright (c) 2015 Regents of the University of California
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

import hbparquet.hadoop.util.ContextUtil;
import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.BlockCompressedInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

public class BAMFilteredRecordReader
    extends BAMRecordReader {

    private static String viewContig;
    private static int viewStart;
    private static int viewEnd;

    public static void setRegion(String contig,
                                 int start,
                                 int end) {
        viewContig = contig;
        viewStart = start;
        viewEnd = end;
    }

    private final LongWritable key = new LongWritable();
    private final SAMRecordWritable record = new SAMRecordWritable();
    
    private ValidationStringency stringency;
    
    private BlockCompressedInputStream bci;
    private BAMRecordCodec codec;
    private long fileStart, virtualEnd;
    private boolean isInitialized = false;
    
    @Override public void initialize(InputSplit spl,
                                     TaskAttemptContext ctx) 
        throws IOException {
        // Check to ensure this method is only be called once (see Hadoop API)
        if (isInitialized) {
            close();
        }
        isInitialized = true;

        Configuration conf = ContextUtil.getConfiguration(ctx);
        FileVirtualSplit split = (FileVirtualSplit) spl;
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(conf);

        this.stringency = SAMHeaderReader.getValidationStringency(conf);

        FSDataInputStream in = fs.open(file);

        // Sets codec to translate between in-memory and disk representation of record
        codec = new BAMRecordCodec(SAMHeaderReader.readSAMHeaderFrom(in, conf));
        
        in.seek(0);

        bci = new BlockCompressedInputStream(new WrapSeekable<FSDataInputStream>(in,
                                                                                 fs.getFileStatus(file).getLen(),
                                                                                 file));

        // Gets BGZF virtual offset for the split
        long virtualStart = split.getStartVirtualOffset();

        fileStart = virtualStart >>> 16;
        virtualEnd = split.getEndVirtualOffset();

        // Starts looking from the BGZF virtual offset
        bci.seek(virtualStart);
        // Reads records from this input stream
        codec.setInputStream(bci);
    }
    
    @Override public void close() throws IOException {
        bci.close();
    }

    @Override public LongWritable getCurrentKey() {
        return key;
    }

    @Override public SAMRecordWritable getCurrentValue() {
        return record;
    }

    /**
     * This method gets the nextKeyValue for our RecordReader, but filters by only
     * returning records within a specified ReferenceRegion.
     */
    @Override public boolean nextKeyValue() {
      while (bci.getFilePointer() < virtualEnd) {
          SAMRecord r = codec.decode();

          // Since we're reading from a BAMRecordCodec directly we have to set the
          // validation stringency ourselves.
          if (this.stringency != null) {
              r.setValidationStringency(this.stringency);
          }

          // This if/else block pushes the predicate down onto a BGZIP block that 
          // the index has said contains data in our specified region.
          if (r == null) {
              return false;
          } else {
              int start = r.getStart();
              int end = r.getEnd();

              if ((r.getContig() == viewContig) &&
                  (((start >= viewStart) && (end <= viewEnd))
                   || ((start <= viewStart) && (end >= viewStart) && (end <= viewEnd))
                   || ((end >= viewEnd) && (start >= viewStart) && (start <= viewEnd)))) {
                  key.set(BAMRecordReader.getKey(r));
                  record.set(r);
                  return true;
              }
          }
      }
      return false;
  }
}
