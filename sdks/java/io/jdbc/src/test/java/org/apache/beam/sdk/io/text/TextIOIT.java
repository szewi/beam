package org.apache.beam.sdk.io.text;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.ParseException;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;



/**
 * A test of {@link org.apache.beam.sdk.io.TextIO}.
 */
@RunWith(JUnit4.class) public class TextIOIT {

  private static final String FILE_BASENAME = "textioit";
  private static final long LINES_OF_TEXT_COUNT = 10000L;

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  @BeforeClass public static void setup() throws SQLException, ParseException {
    //TODO: specify pipeline options etc...
  }

  @AfterClass public static void tearDown() throws SQLException {
    //TODO: cleanup phase. Delete files created during write.
  }

  @Test public void testWriteThenRead() {
    runWrite();
    runRead();
  }

  private void runWrite() {
    pipelineWrite.apply(GenerateSequence.from(0).to(LINES_OF_TEXT_COUNT))
        .apply(MapElements.into(TypeDescriptors.strings()).via(produceTextLine()))
        .apply(TextIO.write().to(FILE_BASENAME));

    pipelineWrite.run().waitUntilFinish();
  }

  private SerializableFunction<Long, String> produceTextLine() {
    return (SerializableFunction<Long, String>) seed -> Hashing.murmur3_128()
        .hashString(seed.toString(), StandardCharsets.UTF_8).toString();
  }

  private void runRead() {
    PCollection<String> files = pipelineRead
        .apply(TextIO.read().from(String.format("%s*", FILE_BASENAME)));

    // TODO: HashingFn requires dependencies on module beam-sdks-java-io-common
    // TODO: which introduces circular dependency (move the module).
    PCollection<String> consolidatedHashcode = files
        .apply(Combine.globally(new HashingFn()).withoutDefaults());

    // TODO: Pre calculated hash values should be stored in separate place not in test body.
    // TODO: Should we precalculate them or calculate them during test runs?
    PAssert.that(consolidatedHashcode).containsInAnyOrder("ccae48ff685c1822e9f4d510363bf018");

    pipelineRead.run().waitUntilFinish();
  }
}

/*
  Next steps:
  - fix circular dependency problem
  - use one pipeline instead of the two (investigation)
  - Should we pre calculate them or calculate them during test runs?
  - test setup & cleanup
  - Better files destination (filesystem? path?)
  - parametrize this test (data amount, filesystem, path)
 */



