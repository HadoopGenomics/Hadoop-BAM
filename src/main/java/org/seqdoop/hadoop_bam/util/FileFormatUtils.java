package org.seqdoop.hadoop_bam.util;

import com.google.common.collect.ImmutableList;
import htsjdk.samtools.util.Interval;
import org.apache.hadoop.conf.Configuration;
import org.seqdoop.hadoop_bam.FormatException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by valentin on 9/20/17.
 */
public abstract class FileFormatUtils {

    /**
     * Returns the list of intervals found in a string configuration property separated by colons.
     * @param conf the source configuration.
     * @param intervalPropertyName the property name holding the intervals.
     * @return {@code null} if there is no such a property in the configuration.
     * @throws NullPointerException if either input is null.
     */
    public static List<Interval> getIntervals(final Configuration conf, final String intervalPropertyName) {
        final String intervalsProperty = conf.get(intervalPropertyName);
        if (intervalsProperty == null) {
            return null;
        }
        if (intervalsProperty.isEmpty()) {
            return ImmutableList.of();
        }
        final List<Interval> intervals = new ArrayList<Interval>();
        for (String s : intervalsProperty.split(",")) {
            final int lastColonIdx = s.lastIndexOf(':');
            if (lastColonIdx < 0) {
                throw new FormatException("no colon found in interval string: " + s);
            }
            final int hyphenIdx = s.indexOf('-', lastColonIdx + 1);
            if (hyphenIdx < 0) {
                throw new FormatException("no hyphen found after colon interval string: " + s);
            }
            intervals.add(
                    new Interval(s.substring(0, lastColonIdx),
                            Integer.parseInt(s.substring(lastColonIdx + 1, hyphenIdx)),
                            Integer.parseInt(s.substring(hyphenIdx + 1))));
        }
        return intervals;
    }
}
