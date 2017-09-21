package org.seqdoop.hadoop_bam;

import htsjdk.samtools.util.Interval;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.seqdoop.hadoop_bam.util.FileFormatUtils;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Unit tests for {@link FileFormatUtils}.
 */
public class FileFormatUtilsTest {

        @Test
        public void testBadInterval() {
            final String[] INVALID_INTERVALS = {
                    "chr1", // full sequence interval are not allowed.
                    "chr1:12", // single position omitting stop is not allowed.
                    "chr1,chr2:121-123", // , are not allowed anywhere
                    "chr20:1,100-3,400", // ,   "             "
                    "MT:35+", // , until end of contig + is not allowed.
                    "MT:13-31-1112", // too many positions.
                    "MT:-2112", // forgot the start position!
                    " MT : 113 - 1245" // blanks are not allowed either.
            };
            for (final String interval : INVALID_INTERVALS) {
                final Configuration conf = new Configuration();
                conf.set("prop-name", interval);
                try {
                    FileFormatUtils.getIntervals(conf, "prop-name");
                    Assert.fail("expected an exception when dealing with '" + interval + "'");
                } catch (final FormatException ex) {
                // fine.
            } catch (final RuntimeException ex) {
                Assert.fail("wrong exception type " + ex.getClass() + " when dealing with '" + interval + "'");
            }
        }
    }

    @Test
    public void testCommonAndFunklyIntervals() {
        final Object[][] VALID_INTERVALS = {
                {"chr1:1-343", "chr1", 1, 343},
                {"chr2:31-145", "chr2", 31, 145},
                {"10:45000012-678901123", "10", 45000012, 678901123},
                {"HLA-DQA1*01:01:02:134-14151", "HLA-DQA1*01:01:02", 134, 14151}};

        final Configuration conf = new Configuration();

        Assert.assertNull(FileFormatUtils.getIntervals(conf, "prop-name"));

        conf.set("prop-name", "");

        Assert.assertNotNull(FileFormatUtils.getIntervals(conf, "prop-name"));
        Assert.assertTrue(FileFormatUtils.getIntervals(conf, "prop-name").isEmpty());

        conf.set("prop-name", Stream.of(VALID_INTERVALS)
                .map(o -> (String) o[0]).collect(Collectors.joining(",")));

        final List<Interval> allIntervals = FileFormatUtils.getIntervals(conf, "prop-name");
        Assert.assertNotNull(allIntervals);
        Assert.assertEquals(allIntervals.size(), VALID_INTERVALS.length);
        for (int i = 0; i < VALID_INTERVALS.length; i++) {
            Assert.assertNotNull(allIntervals.get(i));
            Assert.assertEquals(allIntervals.get(i).getContig(), VALID_INTERVALS[i][1]);
            Assert.assertEquals(allIntervals.get(i).getStart(), VALID_INTERVALS[i][2]);
            Assert.assertEquals(allIntervals.get(i).getEnd(), VALID_INTERVALS[i][3]);
        }
    }

}
