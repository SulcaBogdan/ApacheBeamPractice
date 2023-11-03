package org.example;

import net.bytebuddy.TypeCache;
import org.apache.arrow.flatbuf.Int;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PDone;

import org.joda.time.Duration;


public class Main {

    public static class GetNameFn extends DoFn<String, String> {
        @ProcessElement
        public void processFile(ProcessContext context){
            String row = context.element();
            String[] columns = row.split(",");

            if (columns.length > 2) {
                String fullName = columns[1] + " " + columns[2];
                context.output(fullName);
            }
        }
    }


    public static class GetValsFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void getVals(ProcessContext context) {
            String row = context.element();
            if(!row.contains("Total_Spent")){
                String[] columns = row.split(",");
                if (columns.length > 6) {
                    // Extrage valoarea numerică din coloana corespunzătoare
                    String fullName = columns[1] + " " + columns[2];
                    String numericValue = columns[6];
                    int intValue = Integer.parseInt(numericValue);
                    context.output(KV.of(fullName, intValue));
                }
            }
        }
    }

    public static class GetAccountCard extends DoFn<String, KV<String,String>> {
        @ProcessElement
        public void getAccountCard(ProcessContext context) {
            String row = context.element();
            String[] columns = row.split(",");
            if (columns.length > 4 && columns[4].equals("Issuers")) {
                String fullName = columns[1] + " " + columns[2];
                String comboAccount = "Card Type: " + columns[4];
                context.output(KV.of(fullName, comboAccount));
            }
        }
    }

    public static class FormatAccountCard extends DoFn<KV<String, String>, String>{
        @ProcessElement
        public void formattedAccountCard(ProcessContext context){
            KV<String,String> kv = context.element();
            String formattedCard = kv.getKey() + " " + kv.getValue();
            context.output(formattedCard);
        }

    }


    public static void main(String[] args) {
        //Pipeline pentru wordCount
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipelineNames = Pipeline.create(options);

        PCollection<String> input = pipelineNames
                .apply("Read from txt", TextIO.read().from("gs://bucket_beam_practice/cards.txt"))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(ParDo.of(new GetNameFn()));

        PCollection<KV<String, Long>> wordCounts = input
                //Această transformare este utilizată pentru a număra câte apariții are fiecare element distinct într-o colecție
                .apply(Count.perElement());

        PCollection<String> formattedOutput = wordCounts
                // Specificați că doriți ca rezultatul transformării să fie de tipul String.
                // TypeDescriptors.strings() furnizează un descriptor de tip pentru șiruri de caractere (String).
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via(kv -> kv.getKey() + "," + kv.getValue()));

        PDone writeResult = formattedOutput
                .apply(TextIO.write()
                        .to("gs://bucket_beam_temp1/outputNames")
                        .withSuffix(".txt")
                        .withNumShards(1));

        pipelineNames.run().waitUntilFinish();



        //Pipeline pentru efectuarea calculelor matematice
        Pipeline pipelineSum = Pipeline.create(options);

        // Citim datele și aplicăm transformarea GetValsFn pentru a obține valorile numerice
        PCollection<String> inputVal = pipelineSum.apply("Read data", TextIO.read().from("gs://bucket_beam_practice/cards.txt"));

        PCollection<KV<String, Integer>> nameValuePairs = inputVal.apply(ParDo.of(new GetValsFn()));

        // Efectuăm operația de sumă pe valorile numerice
        PCollection<KV<String, Integer>> summedValues = nameValuePairs
                .apply(Combine.perKey(Sum.ofIntegers()));

        PCollection<String> formattedOutputNew = summedValues.apply(MapElements
                .into(TypeDescriptor.of(String.class))
                .via(kv -> "Name: " + kv.getKey() + ", Total Spent: " + kv.getValue()));


        // Scriem rezultatul sumei în fișierul de ieșire
        PDone output = formattedOutputNew.apply(MapElements.into(TypeDescriptor.of(String.class))
                        .via(sum -> sum))
                .apply(TextIO.write().to("gs://bucket_beam_temp1/outputSum")
                        .withSuffix(".txt")
                        .withNumShards(1));

        pipelineSum.run().waitUntilFinish();


        //Pipeline pentru afisarea tipurilor de card
        Pipeline pipelineCardType = Pipeline.create(options);
        //Citire din document
        PCollection<String> inputCard = pipelineCardType.apply("Reading from file", TextIO.read().from("gs://bucket_beam_practice/cards.txt"));
        //Scoatem perechi de chei si valori cu numele clientului si tipul cardului(American ...)
        PCollection<KV<String, String>> nameCard = inputCard.apply(ParDo.of(new GetAccountCard()));
        //Transformam nameCard din  KV<String, String> in String pentru a putea da output si afisam doar elementele unice folosing Distinct
        PCollection<String> formatFn = nameCard.apply(ParDo.of(new FormatAccountCard())).apply(Distinct.create());
        //Scriem output ul in outputCard.txt
       PDone outputCard = formatFn.apply("Writing file: ", TextIO.write().to("gs://bucket_beam_temp1/outputCard")
                .withSuffix(".txt").withoutSharding());

        pipelineCardType.run();
    }



}





