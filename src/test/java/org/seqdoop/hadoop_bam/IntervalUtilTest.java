package org.seqdoop.hadoop_bam;

import htsjdk.samtools.util.Interval;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.seqdoop.hadoop_bam.util.IntervalUtil;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Unit tests for {@link IntervalUtil}.
 */
public class IntervalUtilTest {

    @Test
    public void testInvalidIntervals() {
        final String[] invalidIntervals = {
                "chr1", // full sequence interval are not allowed.
                "chr1:12", // single position omitting stop is not allowed.
                "chr1,chr2:121-123", // , are not allowed anywhere
                "chr20:1,100-3,400", // ,   "             "
                "MT:35+", // , until end of contig + is not allowed.
                "MT:13-31-1112", // too many positions.
                "MT:-2112", // forgot the start position!
                " MT : 113 - 1245" // blanks are not allowed either.
        };
        for (final String interval : invalidIntervals) {
            final Configuration conf = new Configuration();
            conf.set("prop-name", interval);
            try {
                IntervalUtil.getIntervals(conf, "prop-name");
                Assert.fail("expected an exception when dealing with '" + interval + "'");
            } catch (final FormatException ex) {
                // fine.
            }
        }
    }

    @Test
    public void testValidIntervals() {
        final Object[][] validIntervals = {
                {"chr1:1-343", "chr1", 1, 343}, // standard 'chr' starting contig interval.
                {"chr20_Un:31-145", "chr20_Un", 31, 145}, // standard chromosome name containing underscore.
                {"X:31-145", "X", 31, 145}, // standard 'X' chromosome interval.
                {"10:45000012-678901123", "10", 45000012, 678901123},  // standard number starting chromosome name interval.
                {"HLA-DQA1*01:01:02:134-14151", "HLA-DQA1*01:01:02", 134, 14151}}; // example of a Hg38 assembly
                                                                                   // HLA contigs including - and : in their names.

        final Configuration conf = new Configuration();

        Assert.assertNull(IntervalUtil.getIntervals(conf, "prop-name"));

        conf.set("prop-name", "");

        Assert.assertNotNull(IntervalUtil.getIntervals(conf, "prop-name"));
        Assert.assertTrue(IntervalUtil.getIntervals(conf, "prop-name").isEmpty());

        conf.set("prop-name", Stream.of(validIntervals)
                .map(o -> (String) o[0]).collect(Collectors.joining(",")));

        final List<Interval> allIntervals = IntervalUtil.getIntervals(conf, "prop-name");
        Assert.assertNotNull(allIntervals);
        Assert.assertEquals(allIntervals.size(), validIntervals.length);
        for (int i = 0; i < validIntervals.length; i++) {
            Assert.assertNotNull(allIntervals.get(i));
            Assert.assertEquals(allIntervals.get(i).getContig(), validIntervals[i][1]);
            Assert.assertEquals(allIntervals.get(i).getStart(), validIntervals[i][2]);
            Assert.assertEquals(allIntervals.get(i).getEnd(), validIntervals[i][3]);
        }
    }

}
