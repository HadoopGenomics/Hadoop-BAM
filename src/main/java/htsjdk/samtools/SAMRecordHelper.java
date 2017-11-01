package htsjdk.samtools;

/**
 * This class is required in order to access the protected
 * {@link SAMRecord#eagerDecode()} method in HTSJDK.
 */
public class SAMRecordHelper {
  public static void eagerDecode(SAMRecord record) {
    record.eagerDecode();
  }
}
