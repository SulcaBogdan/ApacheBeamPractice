package org.example;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {
    @Description("Input for the pipeline") // Titlul/descrierea input
    @Default.String("gs://bucket_beam_practice/cards.txt") // Am mentionat de unde provin datele.
    String getInput();
    void setInput(String input);

    @Description("Output for the pipeline") // Titlul/descrierea output
    @Default.String("gs://bucket_beam_temp1/outputCard") // Am mentionat unde vor fi scrise datele.
    String getOutPut();
    void setOutput(String output);
}
