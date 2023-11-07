import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.example.MyOptions;

public class ApacheLearning {
    //Teorie

    /*Apache beam este un model unificat pentru definirea fluxurilor de prelucrare/procesare a datelor batch sau streaming.

    Acesta este alcatuit din:
        -> Pipeline -> este un graph construit de user care arata cum vor fi procesate datele.

        -> PCollection -> este un data set sau un data stream. Datele pe care un 'Pipeline' le proceseaza fac parte
        dintr-un Pcollection.

        -> PTransform -> este un pas sau o actiune in Pipeline. O transformare este aplicata la zero sau mai multe obiecte
         PCollection si genereaza altele noi.
        PCollection este imutabila asadar pentru a modifica sau 'transforma' datele dintr un PCollection se va crea o noua
         copie a PCollection cu transformarile de rigoare.

        -> Aggregation -> inseamna sa faci un calcul folosind informatii din mai multe surse.

        -> User-defined function (UDF) -> cod scrie de user care poate fi folosit pentru a configura modul in care se
        efectueaza o transformare.

        -> Schema -> pentru un PCollection defineste elementele acelei PCollection ca o lista ordonata de campuri denumite

        -> SDK -> este o biblioteca speciala pentru anumite limbaje de programare, care ajuta dezvoltatorii sa creeze
        transformari, sa construiasca pipeline-uri si sa le trimita catre un "runner"(executor).

        -> Runner -> un runner ruleaza un pipeline Beam folosind capacitatile motorului de prelucrare a datelor pe care l-ai ales.

        -> Window -> Un PCollection poate fi impartit pe windows(ferestre) in functie de marcajele temporale ale elementelor
        in functie de marcajele temporale ale elementelor individuale.

        -> Watermark -> reprezinta o estimare a momentului in care se preconizeaza ca toate datele dintr-o anumita fereastra
         sunt asteptate sa ajunga. Acest lucru este necesar deoarece datele nu sunt intotdeauna garantate sa ajunga intr-un pipline
         in ordine cronologica sau sa ajunga intotdeauna la intervale previzibile.

        -> Trigger -> stabileste momentul cand sa agregam rezultatele fiecarei ferestre.

        -> State and timers -> sunt instrumente de baza care iti ofera control total asupra modului in care agreghezi colectiile
        de intrare care se extind in timp, in functie de chei.

        -> Splittable DoFn -> permite procesarea elementelor intr-un mod care nu este momolitic(bloc mare de cod).Poti face
        checkpoint la procesarea unui element, iar executorul poate imparti restul lucrarii pentru a face mai multe lucruri in paralel.

     */

    //Practice steps

    //Create a Pipeline options and object.

    //Pipeline options. Obiect de interfata MyOptions care foloseste optiunile din aceasta.
    MyOptions options = PipelineOptionsFactory.fromArgs().withValidation().as(MyOptions.class);

    //Pipeline object that uses the options we created above.
    Pipeline p = Pipeline.create(options);

    PCollection<String> lines = p.apply("ReadMyFile", TextIO.read().from(options.getInput()));


    //PCollection transforms
    // SYNTAX -> [Final Output PCollection] =
    // [Initial Input PCollection]
    // .apply([First Transform])
    // .apply([Second Transform]
    // .apply([Third Transform]))
    //etc...
    /*

    List of core transforms:
    -ParDo
    -GroupByKey
    -CoGroupByKey
    -Combine
    -Flatten
    -Partition
     */

    /*Am creat o clasa care mosteneste clasa DoFn.
    Aceasta transforma un PCollection<String> intr-un alt PCollection<String>
    Se foloseste adnotarea @ProcessElement
     */
    static class PrintEveryWordFn extends DoFn<String, String>{
        @ProcessElement // Este o adnotare care va fi apelata pentru fiecare element intr un PCollection in timpul procesarii.
        //Este locul unde se defineste logica fiecarui element in parte.
        public void processElement(@Element String word , OutputReceiver<String> out){
            // @Element se folosește pentru a spune că o anumită variabilă este elementul curent pe care îl prelucrezi
            // OutputReceiver -> este un mecanism prin care funcțiile din Apache Beam pot emite rezultate.
            // In cazul nostru se emite un rezultat String catre un PCollection.

            out.output(word);
            //Se foloseste functia output() pentru a emite un cuvant intr-un PCollection.

        }

    }

}
