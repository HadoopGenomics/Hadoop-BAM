package org.seqdoop.hadoop_bam.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CountingOutputStream;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.common.CramVersions;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.seqdoop.hadoop_bam.KeyIgnoringAnySAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMFormat;
import org.seqdoop.hadoop_bam.SplittingBAMIndex;
import org.seqdoop.hadoop_bam.SplittingBAMIndexer;

/**
 * Merges headerless BAM or CRAM files produced by {@link KeyIgnoringAnySAMOutputFormat}
 * into a single file.
 */
public class SAMFileMerger {

  private SAMFileMerger() {
  }

  /**
   * Merge part file shards produced by {@link KeyIgnoringAnySAMOutputFormat} into a
   * single file with the given header.
   * @param partDirectory the directory containing part files
   * @param outputFile the file to write the merged file to
   * @param samOutputFormat the format (must be BAM or CRAM; SAM is not supported)
   * @param header the header for the merged file
   * @throws IOException
   */
  public static void mergeParts(final String partDirectory, final String outputFile,
      final SAMFormat samOutputFormat, final SAMFileHeader header) throws IOException {

    final String outputParentDir = partDirectory;
    // First, check for the _SUCCESS file.
    final String successFile = partDirectory + "/_SUCCESS";
    final org.apache.hadoop.fs.Path successPath = new org.apache.hadoop.fs.Path(successFile);
    final Configuration conf = new Configuration();

    //it's important that we get the appropriate file system by requesting it from the path
    final FileSystem fs = successPath.getFileSystem(conf);
    if (!fs.exists(successPath)) {
      throw new NoSuchFileException(successFile, null, "Unable to find _SUCCESS file");
    }
    final org.apache.hadoop.fs.Path partPath = new org.apache.hadoop.fs.Path(partDirectory);
    final org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputFile);
    final org.apache.hadoop.fs.Path tmpPath = new org.apache.hadoop.fs.Path(outputParentDir + "tmp" + UUID.randomUUID());

    fs.rename(partPath, tmpPath);
    fs.delete(outputPath, true);

    long headerLength;
    try (final CountingOutputStream out =
             new CountingOutputStream(fs.create(outputPath))) {
      if (header != null) {
        new SAMOutputPreparer().prepareForRecords(out, samOutputFormat, header); // write the header

      }
      headerLength = out.getCount();
      mergeInto(out, tmpPath, conf);
      writeTerminatorBlock(out, samOutputFormat);
    }
    long fileLength = fs.getFileStatus(outputPath).getLen();

    final org.apache.hadoop.fs.Path outputSplittingBaiPath = outputPath.suffix
        (SplittingBAMIndexer.OUTPUT_FILE_EXTENSION);
    try (final OutputStream out = fs.create(outputSplittingBaiPath)) {
      mergeSplittingBaiFiles(out, tmpPath, conf, headerLength, fileLength);
    } catch (IOException e) {
      fs.delete(outputSplittingBaiPath, false);
    }

    fs.delete(tmpPath, true);

  }

  static void mergeInto(OutputStream out, org.apache.hadoop.fs.Path directory, Configuration conf) throws IOException {
    final FileSystem fs = directory.getFileSystem(conf);
    final FileStatus[] parts = getFragments(directory, fs);

    if( parts.length == 0){
      throw new IllegalArgumentException("Could not write bam file because no part files were found.");
    }

    for (final FileStatus part : parts) {
      try (final InputStream in = fs.open(part.getPath())) {
        org.apache.hadoop.io.IOUtils.copyBytes(in, out, conf, false);
      }
    }
    for (final FileStatus part : parts) {
      fs.delete(part.getPath(), false);
    }
  }

  @VisibleForTesting
  static FileStatus[] getFragments( final org.apache.hadoop.fs.Path directory, final FileSystem fs ) throws IOException {
    final FileStatus[] parts = fs.globStatus(new org.apache.hadoop.fs.Path(directory,
        "part-[mr]-[0-9][0-9][0-9][0-9][0-9]*"),
        path -> !path.getName().endsWith(SplittingBAMIndexer.OUTPUT_FILE_EXTENSION));

    // FileSystem.globStatus() has a known bug that causes it to not sort the array returned by
    // name (despite claiming to): https://issues.apache.org/jira/browse/HADOOP-10798
    // Because of this bug, we do an explicit sort here to avoid assembling the bam fragments
    // in the wrong order.
    Arrays.sort(parts);

    return parts;
  }

  //Terminate the aggregated output stream with an appropriate SAMOutputFormat-dependent terminator block
  private static void writeTerminatorBlock(final OutputStream out, final SAMFormat samOutputFormat) throws IOException {
    if (SAMFormat.CRAM == samOutputFormat) {
      CramIO.issueEOF(CramVersions.DEFAULT_CRAM_VERSION, out); // terminate with CRAM EOF container
    }
    else {
      out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK); // add the BGZF terminator
    }
  }

  static void mergeSplittingBaiFiles(OutputStream out, org.apache.hadoop.fs.Path
      directory, Configuration conf, long headerLength, long fileLength) throws
      IOException {
    final FileSystem fs = directory.getFileSystem(conf);
    final FileStatus[] parts = getSplittingBaiFiles(directory, fs);

    if( parts.length == 0){
      return; // nothing to merge
    }

    List<Long> mergedVirtualOffsets = new ArrayList<>();
    long partFileOffset = headerLength;
    for (final FileStatus part : parts) {
      try (final InputStream in = fs.open(part.getPath())) {
        SplittingBAMIndex index = new SplittingBAMIndex(in);
        LinkedList<Long> virtualOffsets = new LinkedList<>(index.getVirtualOffsets());
        for (int i = 0; i < virtualOffsets.size() - 1; i++) {
          long virtualOffset = virtualOffsets.get(i);
          mergedVirtualOffsets.add(shiftVirtualFilePointer(virtualOffset, partFileOffset));
        }
        long partLength = virtualOffsets.getLast();
        partFileOffset += (partLength >> 16);
      }
    }


    SplittingBAMIndexer splittingBAMIndexer = new SplittingBAMIndexer(out);
    for (Long offset : mergedVirtualOffsets) {
      splittingBAMIndexer.writeVirtualOffset(offset);
    }
    splittingBAMIndexer.finish(partFileOffset);
    int terminatingBlockLength = BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK.length;
    if (partFileOffset + terminatingBlockLength != fileLength) {
      throw new IOException("Part file length mismatch. Last part file offset is " +
          partFileOffset + ", expected: " + (fileLength - terminatingBlockLength));
    }

    for (final FileStatus part : parts) {
      fs.delete(part.getPath(), false);
    }
  }

  private static long shiftVirtualFilePointer(long virtualFilePointer, long offset) {
    long blockAddress = BlockCompressedFilePointerUtil.getBlockAddress(virtualFilePointer);
    int blockOffset = BlockCompressedFilePointerUtil.getBlockOffset(virtualFilePointer);
    return (blockAddress + offset) << 16 | (long) blockOffset;
  }

  static FileStatus[] getSplittingBaiFiles( final org.apache.hadoop.fs.Path directory, final FileSystem fs ) throws IOException {
    final FileStatus[] parts = fs.globStatus(new org.apache.hadoop.fs.Path(directory,
            "part-[mr]-[0-9][0-9][0-9][0-9][0-9]*" + SplittingBAMIndexer.OUTPUT_FILE_EXTENSION));

    // FileSystem.globStatus() has a known bug that causes it to not sort the array returned by
    // name (despite claiming to): https://issues.apache.org/jira/browse/HADOOP-10798
    // Because of this bug, we do an explicit sort here to avoid assembling the bam fragments
    // in the wrong order.
    Arrays.sort(parts);

    return parts;
  }
}
