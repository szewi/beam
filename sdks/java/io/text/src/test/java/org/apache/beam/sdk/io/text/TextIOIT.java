package org.apache.beam.sdk.io.text;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** A test of {@link org.apache.beam.sdk.io.TextIO}. */
@RunWith(JUnit4.class)
public class TextIOIT {

  private static final String FILE_BASENAME = "textioit";
  private static long linesOfTextCount = 10000L;
  private static final String EXPECTED_HASHCODE = "ccae48ff685c1822e9f4d510363bf018";

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws ParseException {
    PipelineOptionsFactory.register(IOTextTestPipelineOptions.class);
    IOTextTestPipelineOptions options = TestPipeline.testingPipelineOptions()
            .as(IOTextTestPipelineOptions.class);

    if (options.getTestSize() != null) {
      linesOfTextCount = options.getTestSize();
    }
  }

  @Test
  public void testWriteThenRead() {

    PCollection<String> consolidatedContentHashcode = pipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(linesOfTextCount))
        .apply("Produce text", MapElements.into(TypeDescriptors.strings()).via(produceTextLine()))
        .apply("Write content to files", TextIO.write().to(FILE_BASENAME).withOutputFilenames())
        .getPerDestinationOutputFilenames()
        .apply("Read all files", Values.create()).apply(TextIO.readAll())
        .apply("Calculate hashcode", Combine.globally(new HashingFn()).withoutDefaults());

    assertHashcodeOk(consolidatedContentHashcode);

    pipeline.run().waitUntilFinish();
  }

  private SerializableFunction<Long, String> produceTextLine() {
    return (SerializableFunction<Long, String>)
        seed ->
            Hashing.murmur3_128().hashString(seed.toString(), StandardCharsets.UTF_8).toString();
  }

  private void assertHashcodeOk(PCollection<String> consolidatedContentHashcode) {
    PAssert.that(consolidatedContentHashcode).containsInAnyOrder(EXPECTED_HASHCODE);
  }
}

/*
 TODO:
 Next steps:
 - test setup & cleanup
 - Better files destination (filesystem? path?)
 - parametrize this test (data amount, filesystem, path)
*/
