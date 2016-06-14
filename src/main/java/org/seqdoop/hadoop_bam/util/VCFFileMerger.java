package org.seqdoop.hadoop_bam.util;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;
import java.io.BufferedInputStream;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFFormat;

/**
 * Merges headerless VCF files produced by {@link KeyIgnoringVCFOutputFormat}
 * into a single file.
 */
public class VCFFileMerger {
  /**
   * Merge part file shards produced by {@link KeyIgnoringVCFOutputFormat} into a
   * single file with the given header.
   * @param partDirectory the directory containing part files
   * @param outputFile the file to write the merged file to
   * @param header the header for the merged file
   * @throws IOException
   */
  public static void mergeParts(final String partDirectory, final String outputFile,
      final VCFHeader header) throws IOException {
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

    try (final OutputStream out = Files.newOutputStream(outputPath)) {
      boolean blockCompressed = writeHeader(out, outputPath, partPath, header);
      mergeInto(out, partPath);
      if (blockCompressed) {
        writeTerminatorBlock(out);
      }
    }

    deleteRecursive(partPath);
  }

  private static Path asPath(String path) {
    URI uri = URI.create(path);
    return uri.getScheme() == null ? Paths.get(path) : Paths.get(uri);
  }

  /**
   * @return whether the output is block compressed
   */
  private static boolean writeHeader(OutputStream out, Path outputPath, Path partPath,
      VCFHeader header) throws IOException {
    if (header == null) {
      return false;
    }
    boolean blockCompressed = isBlockCompressed(partPath);
    boolean bgzExtension = outputPath.toString().endsWith(BGZFCodec.DEFAULT_EXTENSION);
    if (blockCompressed && !bgzExtension) {
      System.err.println("WARNING: parts are block compressed, but output does not " +
          "have .bgz extension: " + outputPath);
    } else if (!blockCompressed && bgzExtension) {
      System.err.println("WARNING: parts are not block compressed, but output has a " +
          ".bgz extension: " + outputPath);
    }
    boolean gzipCompressed = isGzipCompressed(partPath);
    OutputStream headerOut;
    if (blockCompressed) {
      headerOut = new BlockCompressedOutputStream(out, null);
    } else if (gzipCompressed) {
      headerOut = new GZIPOutputStream(out);
    } else {
      headerOut = out;
    }
    VariantContextWriter writer = new VariantContextWriterBuilder().clearOptions()
        .setOutputVCFStream(headerOut).build();
    writer.writeHeader(header);
    headerOut.flush();
    if (headerOut instanceof GZIPOutputStream) {
      ((GZIPOutputStream) headerOut).finish();
    }
    return blockCompressed;
  }

  private static boolean isBlockCompressed(Path directory) throws IOException {
    final List<Path> parts = getFragments(directory);
    try (InputStream in = new BufferedInputStream(Files.newInputStream(parts.get(0)))) {
      return BlockCompressedInputStream.isValidFile(in);
    }
  }

  private static boolean isGzipCompressed(Path directory) throws IOException {
    final List<Path> parts = getFragments(directory);
    try (InputStream in = new BufferedInputStream(Files.newInputStream(parts.get(0)))) {
      return VCFFormat.isGzip(in);
    }
  }

  private static void mergeInto(OutputStream out, Path directory) throws IOException {
    final List<Path> parts = getFragments(directory);
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
        .filter(path -> !path.toString().endsWith(TabixUtils.STANDARD_INDEX_EXTENSION))
        .collect(Collectors.toList());
    Collections.sort(parts);
    if (parts.isEmpty()) {
      throw new IllegalArgumentException("Could not write vcf file because no part " +
          "files were found in " + directory);
    }
    return parts;
  }

  private static void writeTerminatorBlock(final OutputStream out) throws IOException {
    out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK); // add the BGZF terminator
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
}
