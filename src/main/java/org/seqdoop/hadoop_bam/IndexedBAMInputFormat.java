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
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.PublicBAMFileSpan;
import htsjdk.samtools.PublicDiskBasedBAMFileIndex;
import htsjdk.samtools.SAMSequenceDictionary;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

public class IndexedBAMInputFormat
    extends BAMInputFormat {

    private static Path filePath;
    private static Path indexFilePath;
    private static String viewContig;
    private static int viewStart;
    private static int viewEnd;
    private static SAMSequenceDictionary dict;

    public static void setVars(Path bamPath,
                               Path baiPath,
                               String contig,
                               int start,
                               int end,
                               SAMSequenceDictionary sd) {
        filePath = bamPath;
        indexFilePath = baiPath;
        viewContig = contig;
        viewStart = start;
        viewEnd = end;
        dict = sd;
    }

    @Override public RecordReader<LongWritable, SAMRecordWritable>
        createRecordReader(InputSplit split,
                           TaskAttemptContext ctx) 
        throws IOException, InterruptedException {
        RecordReader<LongWritable, SAMRecordWritable> rr = new BAMFilteredRecordReader();
        BAMFilteredRecordReader.setRegion(viewContig, viewStart, viewEnd);
        rr.initialize(split, ctx);
        return rr;
    }

    @Override public List<InputSplit> getSplits(JobContext job)
        throws IOException, FileNotFoundException {

        Configuration conf = ContextUtil.getConfiguration(job);
        FileSystem fs = indexFilePath.getFileSystem(conf);

        if (!fs.exists(indexFilePath)) {
            throw new java.io.FileNotFoundException("Bam index file not provided");
        } else {
            WrapSeekable seekableIdxFile = WrapSeekable.openPath(fs,
                                                                 indexFilePath);

            // Use index to get the chunks for a specific region, then use them to create InputSplits
            PublicDiskBasedBAMFileIndex idx = new PublicDiskBasedBAMFileIndex(seekableIdxFile,
                                                                              dict);
            int referenceIndex = dict.getSequenceIndex(viewContig);

            // Get the chunks in the region we want (chunks give start and end file pointers into a BAM file)
            SAMFileSpan sfs = idx.getSpanOverlapping(referenceIndex, viewStart, viewEnd);
            List<Chunk> regions = PublicBAMFileSpan.getChunks(sfs);
            Iterator<Chunk> ri = regions.iterator();
            List<InputSplit> splits = new LinkedList<InputSplit>();
            while (ri.hasNext()) {          
                Chunk chunk = ri.next();

                // Create InputSplits from chunks in a given region
                long start = chunk.getChunkStart();
                long end = chunk.getChunkEnd();
                String[] array = new String[0];
                splits.add(new FileVirtualSplit(filePath, start, end, array));
            }

            return splits;
        }
    }
}
