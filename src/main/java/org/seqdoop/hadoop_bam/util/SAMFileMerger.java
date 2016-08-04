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
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
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

    // First, check for the _SUCCESS file.
    final String successFile = partDirectory + "/_SUCCESS";
    final Path successPath = asPath(successFile);
    if (!Files.exists(successPath)) {
      throw new NoSuchFileException(successFile, null, "Unable to find _SUCCESS file");
    }
    final Path partPath = asPath(partDirectory);
    final Path outputPath = asPath(outputFile);
    if (partPath.equals(outputPath)) {
      throw new IllegalArgumentException("Cannot merge parts into output with same " +
          "path: " + partPath);
    }

    Files.deleteIfExists(outputPath);

    long headerLength;
    try (final CountingOutputStream out =
             new CountingOutputStream(Files.newOutputStream(outputPath))) {
      if (header != null) {
        new SAMOutputPreparer().prepareForRecords(out, samOutputFormat, header); // write the header
      }
      headerLength = out.getCount();
      mergeInto(out, partPath);
      writeTerminatorBlock(out, samOutputFormat);
    }
    long fileLength = Files.size(outputPath);

    final Path outputSplittingBaiPath = outputPath.resolveSibling(
        outputPath.getFileName() + SplittingBAMIndexer.OUTPUT_FILE_EXTENSION);
    try (final OutputStream out = Files.newOutputStream(outputSplittingBaiPath)) {
      mergeSplittingBaiFiles(out, partPath, headerLength, fileLength);
    } catch (IOException e) {
      deleteRecursive(outputSplittingBaiPath);
    }

    deleteRecursive(partPath);
  }

  private static Path asPath(String path) {
    URI uri = URI.create(path);
    return uri.getScheme() == null ? Paths.get(path) : Paths.get(uri);
  }

  private static void deleteRecursive(Path directory) throws IOException {
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }
      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private static void mergeInto(OutputStream out, Path directory) throws IOException {
    final List<Path> parts = getFragments(directory);
    if (parts.isEmpty()) {
      throw new IllegalArgumentException("Could not write bam file because no part " +
          "files were found in " + directory);
    }
    for (final Path part : parts) {
      Files.copy(part, out);
    }
    for (final Path part : parts) {
      Files.delete(part);
    }
  }

  @VisibleForTesting
  static List<Path> getFragments(final Path directory) throws IOException {
    PathMatcher matcher = directory.getFileSystem().getPathMatcher("glob:**/part-[mr]-[0-9][0-9][0-9][0-9][0-9]*");
    List<Path> parts = Files.walk(directory)
        .filter(matcher::matches)
        .filter(path -> !path.toString().endsWith(SplittingBAMIndexer.OUTPUT_FILE_EXTENSION))
        .collect(Collectors.toList());
    Collections.sort(parts);
    return parts;
  }

  //Terminate the aggregated output stream with an appropriate SAMOutputFormat-dependent terminator block
  private static void writeTerminatorBlock(final OutputStream out, final SAMFormat samOutputFormat) throws IOException {
    if (SAMFormat.CRAM == samOutputFormat) {
      CramIO.issueEOF(CramVersions.DEFAULT_CRAM_VERSION, out); // terminate with CRAM EOF container
    } else {
      out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK); // add the BGZF terminator
    }
  }

  static void mergeSplittingBaiFiles(OutputStream out, Path directory, long headerLength,
      long fileLength) throws IOException {
    final List<Path> parts = getSplittingBaiFiles(directory);

    if (parts.isEmpty()) {
      return; // nothing to merge
    }

    List<Long> mergedVirtualOffsets = new ArrayList<>();
    long partFileOffset = headerLength;
    for (final Path part : parts) {
      try (final InputStream in = Files.newInputStream(part)) {
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

    for (final Path part : parts) {
      Files.delete(part);
    }
  }

  private static long shiftVirtualFilePointer(long virtualFilePointer, long offset) {
    long blockAddress = BlockCompressedFilePointerUtil.getBlockAddress(virtualFilePointer);
    int blockOffset = BlockCompressedFilePointerUtil.getBlockOffset(virtualFilePointer);
    return (blockAddress + offset) << 16 | (long) blockOffset;
  }

  static List<Path> getSplittingBaiFiles(final Path directory) throws IOException {
    PathMatcher matcher = directory.getFileSystem()
        .getPathMatcher("glob:**/part-[mr]-[0-9][0-9][0-9][0-9][0-9]*" + SplittingBAMIndexer.OUTPUT_FILE_EXTENSION);
    List<Path> parts = Files.walk(directory).filter(matcher::matches).collect(Collectors.toList());
    Collections.sort(parts);
    return parts;
  }
}
