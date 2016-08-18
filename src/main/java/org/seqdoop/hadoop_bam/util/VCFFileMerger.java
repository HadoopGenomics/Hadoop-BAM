package org.seqdoop.hadoop_bam.util;

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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFFormat;

import static org.seqdoop.hadoop_bam.util.NIOFileUtil.asPath;
import static org.seqdoop.hadoop_bam.util.NIOFileUtil.deleteRecursive;
import static org.seqdoop.hadoop_bam.util.NIOFileUtil.getFilesMatching;
import static org.seqdoop.hadoop_bam.util.NIOFileUtil.mergeInto;

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

    List<Path> parts = getFilesMatching(partPath,
        "glob:**/part-[mr]-[0-9][0-9][0-9][0-9][0-9]*",
        TabixUtils.STANDARD_INDEX_EXTENSION);
    if (parts.isEmpty()) {
      throw new IllegalArgumentException("Could not write bam file because no part " +
          "files were found in " + partPath);
    }

    Files.deleteIfExists(outputPath);

    try (final OutputStream out = Files.newOutputStream(outputPath)) {
      boolean blockCompressed = writeHeader(out, outputPath, parts, header);
      mergeInto(parts, out);
      if (blockCompressed) {
        writeTerminatorBlock(out);
      }
    }

    deleteRecursive(partPath);
  }

  /**
   * @return whether the output is block compressed
   */
  private static boolean writeHeader(OutputStream out, Path outputPath, List<Path> parts,
      VCFHeader header) throws IOException {
    if (header == null) {
      return false;
    }
    boolean blockCompressed = isBlockCompressed(parts);
    boolean bgzExtension = outputPath.toString().endsWith(BGZFCodec.DEFAULT_EXTENSION);
    if (blockCompressed && !bgzExtension) {
      System.err.println("WARNING: parts are block compressed, but output does not " +
          "have .bgz extension: " + outputPath);
    } else if (!blockCompressed && bgzExtension) {
      System.err.println("WARNING: parts are not block compressed, but output has a " +
          ".bgz extension: " + outputPath);
    }
    boolean gzipCompressed = isGzipCompressed(parts);
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

  private static boolean isBlockCompressed(List<Path> parts) throws IOException {
    try (InputStream in = new BufferedInputStream(Files.newInputStream(parts.get(0)))) {
      return BlockCompressedInputStream.isValidFile(in);
    }
  }

  private static boolean isGzipCompressed(List<Path> parts) throws IOException {
    try (InputStream in = new BufferedInputStream(Files.newInputStream(parts.get(0)))) {
      return VCFFormat.isGzip(in);
    }
  }

  private static void writeTerminatorBlock(final OutputStream out) throws IOException {
    out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK); // add the BGZF terminator
  }
}
