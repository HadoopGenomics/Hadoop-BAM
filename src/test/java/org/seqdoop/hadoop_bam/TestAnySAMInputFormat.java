package org.seqdoop.hadoop_bam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestAnySAMInputFormat {

    @Test
    public void testHeaderlessSamFormat() throws PathNotFoundException {
        final SAMFormat result = getSamFormat(new Configuration(), "test_headerless.sam");
        assertEquals(SAMFormat.SAM, result);
    }

    @Test
    public void testTrustExtensionsIsHonored() throws PathNotFoundException {
        final Configuration conf = new Configuration();
        //default to trusting exceptions
        assertEquals(SAMFormat.SAM, getSamFormat(conf, "misnamedBam.sam"));

        conf.set(AnySAMInputFormat.TRUST_EXTS_PROPERTY, "false");
        final SAMFormat result = getSamFormat(conf, "misnamedBam.sam");
        assertEquals(SAMFormat.BAM, result);
    }

    private SAMFormat getSamFormat(final Configuration conf, final String file) throws PathNotFoundException {
        final String filePath = getClass().getClassLoader().getResource(file).getFile();
        return new AnySAMInputFormat(conf).getFormat(new Path(filePath));
    }
}
