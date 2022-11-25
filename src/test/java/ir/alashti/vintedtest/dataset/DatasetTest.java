package ir.alashti.vintedtest.dataset;

import ir.alashti.vintedtest.dataset.input.Loader;
import ir.alashti.vintedtest.dataset.udf.Tuple2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DatasetTest<T> {

    Dataset<T> dataset;

    @BeforeEach
    void setUp() {
        dataset = new Loader(Paths.get("data/clicks/part-001.csv"), null, true, Formats.CSV.getSplitter()).call();
    }

    @Test
    void map() {
        HashSet<Object> objects = new HashSet<>(dataset.map(d -> ((Row) d).getValues().get(0)).getRows());
        Assertions.assertEquals(8, objects.size());
    }

    @Test
    void mapToPair() {
        PairDataset<String, Integer> pairDataset = dataset.mapToPair(d -> {
            List<Object> values = ((Row) d).getValues();
            return new Tuple2<>((String) values.get(0), Integer.parseInt((String) values.get(1)));
        }).reduce((k, d1, d2) -> d1 + d2);
        Assertions.assertEquals(8, pairDataset.count());
    }

    @Test
    void select() {
        List<Object> user_id1 = dataset.select("user_id").map(d -> ((Row) d).getValues().get(0)).getRows();
        Set<Integer> collect = user_id1.stream().map(d -> Integer.parseInt((String) d)).collect(Collectors.toSet());
        Assertions.assertEquals(11, dataset.count());
        Assertions.assertEquals(1, collect.size());
    }

    @Test
    void filter() {
        Assertions.assertEquals(3, dataset.filter(d -> ((String) ((Row) d).getValues().get(2)).equals("profile")).count());
    }

    @Test
    void count() {
        Assertions.assertEquals(11, dataset.count());
    }

    @Test
    void merge() {
        Dataset dataset2 = new Loader(Paths.get("data/clicks/part-002.csv"), null, true, Formats.CSV.getSplitter()).call();
        long expected = dataset.count() + dataset2.count();
        dataset.merge(dataset2);
        Assertions.assertEquals(expected, dataset.count());
    }
}