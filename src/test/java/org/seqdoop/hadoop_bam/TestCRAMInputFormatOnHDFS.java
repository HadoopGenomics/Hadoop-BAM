package org.seqdoop.hadoop_bam;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestCRAMInputFormatOnHDFS {
  private String input;
  private String reference;
  private TaskAttemptContext taskAttemptContext;
  private JobContext jobContext;


  private static MiniDFSCluster cluster;
  private static URI clusterUri;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = startMini(TestCRAMInputFormatOnHDFS.class.getName());
    clusterUri = formalizeClusterURI(cluster.getFileSystem().getUri());
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    if (cluster != null)
    {
      cluster.shutdown();
    }
  }


  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    input = ClassLoader.getSystemClassLoader().getResource("test.cram").getFile();
    reference = ClassLoader.getSystemClassLoader().getResource("auxf.fa").toURI().toString();
    String referenceIndex = ClassLoader.getSystemClassLoader().getResource("auxf.fa.fai")
        .toURI().toString();
    conf.set("mapred.input.dir", "file://" + input);

    URI hdfsRef = clusterUri.resolve("/tmp/auxf.fa");
    URI hdfsRefIndex = clusterUri.resolve("/tmp/auxf.fa.fai");
    Files.copy(Paths.get(URI.create(reference)), Paths.get(hdfsRef));
    Files.copy(Paths.get(URI.create(referenceIndex)), Paths.get(hdfsRefIndex));

    conf.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, hdfsRef.toString());


    taskAttemptContext = new TaskAttemptContextImpl(conf, mock(TaskAttemptID.class));
    jobContext = new JobContextImpl(conf, taskAttemptContext.getJobID());

  }

  private static MiniDFSCluster startMini(String testName) throws IOException {
    File baseDir = new File("./target/hdfs/" + testName).getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    MiniDFSCluster hdfsCluster = builder.clusterId(testName).build();
    hdfsCluster.waitActive();
    return hdfsCluster;
  }

  protected static URI formalizeClusterURI(URI clusterUri) throws URISyntaxException {
    if (clusterUri.getPath()==null) {
      return new URI(clusterUri.getScheme(), null,
          clusterUri.getHost(), clusterUri.getPort(),
          "/", null, null);
    } else if (clusterUri.getPath().trim()=="") {
      return new URI(clusterUri.getScheme(), null,
          clusterUri.getHost(), clusterUri.getPort(),
          "/", null, null);
    }
    return clusterUri;
  }

  @Test
  public void testReader() throws Exception {
    int expectedCount = 0;
    SamReader samReader = SamReaderFactory.makeDefault()
        .referenceSequence(new File(URI.create(reference))).open(new File(input));
    for (SAMRecord r : samReader) {
      expectedCount++;
    }

    CRAMInputFormat inputFormat = new CRAMInputFormat();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);
    assertEquals(1, splits.size());
    RecordReader<LongWritable, SAMRecordWritable> reader = inputFormat
        .createRecordReader(splits.get(0), taskAttemptContext);
    reader.initialize(splits.get(0), taskAttemptContext);

    int actualCount = 0;
    while (reader.nextKeyValue()) {
      actualCount++;
    }

    assertEquals(expectedCount, actualCount);
  }

}
