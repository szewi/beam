package org.apache.beam.sdk.io.text;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * Pipeline options specific for filebased IO tests.
 */

public interface IOTextTestPipelineOptions extends TestPipelineOptions{
    @Description("Test row count")
    @Default.Long(10000)
    Long getTestSize();
    void setTestSize(Long value);
}
