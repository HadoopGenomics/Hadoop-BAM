package org.seqdoop.hadoop_bam.util;

import com.google.common.collect.ImmutableList;
import htsjdk.samtools.util.Interval;
import org.apache.hadoop.conf.Configuration;
import org.seqdoop.hadoop_bam.FormatException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Common utilities across different file formats.
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
        final List<Interval> intervals = new ArrayList<>();
        for (String s : intervalsProperty.split(",")) {
            final int lastColonIdx = s.lastIndexOf(':');
            if (lastColonIdx < 0) {
                throw new FormatException("no colon found in interval string: " + s);
            }
            final int hyphenIdx = s.indexOf('-', lastColonIdx + 1);
            if (hyphenIdx < 0) {
                throw new FormatException("no hyphen found after colon interval string: " + s);
            }
            final String sequence = s.substring(0, lastColonIdx);
            final int start = parseIntOrThrowFormatException(s.substring(lastColonIdx + 1, hyphenIdx),
                    () -> "invalid start position in '" + s + "'");
            final int stop = parseIntOrThrowFormatException(s.substring(hyphenIdx + 1),
                    () -> "invalid stop position in '" + s + "'");
            intervals.add(new Interval(sequence, start, stop));
        }
        return intervals;
    }

    private static int parseIntOrThrowFormatException(final String str, final Supplier<String> exceptionMessage) {
        try {
            return Integer.parseInt(str);
        } catch (final NumberFormatException ex) {
            throw new FormatException(exceptionMessage.get());
        }
    }
}
