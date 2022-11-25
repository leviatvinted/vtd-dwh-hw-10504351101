package ir.alashti.vintedtest.dataset.parallel;

import ir.alashti.vintedtest.dataset.PairDataset;
import ir.alashti.vintedtest.dataset.udf.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Shuffler {
    public static <K, V> Map<Integer, PairDataset<K, V>> shuffleByKey(List<Tuple2<K, V>> collect) {
        Map<Integer, PairDataset<K, V>> shuffleDataset = new HashMap<>();
        for (Tuple2<K, V> kvTuple2 : collect) {
            int distKey = kvTuple2.getKey().hashCode();
            if(shuffleDataset.containsKey(distKey)) {
                PairDataset<K, V> pairDataset = shuffleDataset.get(distKey);
                if(pairDataset.getRows().containsKey(kvTuple2.getKey())) {
                    List<V> v1s = pairDataset.getRows().get(kvTuple2.getKey());
                    v1s.add((V) kvTuple2.getValue());
                } else {
                    List<V> v1s = new ArrayList<>();
                    v1s.add((V) kvTuple2.getValue());
                    pairDataset.getRows().put((K) kvTuple2.getKey(), v1s);
                }
            } else {
                PairDataset<K, V> pairDataset = new PairDataset<>();
                List<V> v1s = new ArrayList<>();
                v1s.add((V) kvTuple2.getValue());
                pairDataset.getRows().put((K) kvTuple2.getKey(), v1s);
                shuffleDataset.put(distKey, pairDataset);
            }
        }
        return shuffleDataset;
    }
}
