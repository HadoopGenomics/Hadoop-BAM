// File created: 2010-08-20 13:54:10

import java.io.File;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.BAMFileWriter;

public final class GetSortedBAMHeader {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println(
				"Usage: GetSortedBAMHeader input output\n\n"+

				"Reads the BAM header from input (a standard BGZF-compressed BAM "+
				"file), and\nwrites it (BGZF-compressed) to output. Sets the "+
				"sort order indicated in the SAM\nheader to 'coordinate'.");
			System.exit(1);
		}

		final BAMFileWriter w = new BAMFileWriter(new File(args[1]));
		w.setSortOrder(SAMFileHeader.SortOrder.coordinate, true);
		w.setHeader(new SAMFileReader(new File(args[0])).getFileHeader());
		w.close();
	}
}
