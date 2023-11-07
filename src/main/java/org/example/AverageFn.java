package org.example;

import org.apache.beam.sdk.transforms.Combine;

public class AverageFn extends Combine.CombineFn<Integer, AverageFn.Accum, Double> {

/*
Se creaza clasa AverageFn care este o subclasa a CombineFn. Aceasta subclasa va primi valori Integer, va tine un acumulator de tip Accum si
va produce un rezultat de tip Double.
*/

    public static class Accum { //Aici se tine evidenta sumei si a numarului de elemente (count).
        int sum = 0;
        int count = 0;

    }
    @Override
    public Accum createAccumulator(){ // Metoda suprascrisa din super clasa CombineFn pentru a crea un obiect Accum()
        return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Integer input){ // Metoda suprascrisa din super clasa CombineFn care adauga la sum si count val int
        accum.sum += input;
        accum.count++;
        return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums){ // Metoda suprascrisa din super clasa CombineFn care combina doua acumulatoare.
        Accum merged = createAccumulator();
        for (Accum accum : accums){
            merged.sum  += accum.sum;
        }
        return merged;
    }

    @Override
    public Double extractOutput(Accum accum){ // Metoda suprascrisa din super clasa CombineFn care returneaza media rezultatului final.
        return ((double) accum.sum) / accum.count;
    }



}
