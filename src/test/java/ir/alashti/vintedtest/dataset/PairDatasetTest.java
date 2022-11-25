package ir.alashti.vintedtest.dataset;

import ir.alashti.vintedtest.dataset.input.Loader;
import ir.alashti.vintedtest.dataset.input.LoaderManager;
import ir.alashti.vintedtest.dataset.parallel.ParallelDataset;
import ir.alashti.vintedtest.dataset.parallel.ParallelPairDataset;
import ir.alashti.vintedtest.dataset.udf.Tuple2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PairDatasetTest {
    PairDataset<Long, Row> clicks;
    ParallelPairDataset<Long, Row> users;

    @BeforeEach
    public void setUp() {
        List<String> clicksHeader = new ArrayList<>();
        clicksHeader.add("date");
        clicksHeader.add("screen");
        clicksHeader.add("user_id");
        clicksHeader.add("click_target");
        Dataset<Row> ds = new Loader(Paths.get("data/clicks/part-001.csv"), clicksHeader, true, Formats.CSV.getSplitter()).call();

        clicks = ds.mapToPair(d -> {
            Row d1 = (Row) d;
            List<Object> values = d1.getValues();
            Tuple2<Long, Row> longRowTuple2 = new Tuple2<>(Long.parseLong((String) values.get(2)), d1);
            values.remove(2);
            return longRowTuple2;
        });

        List<String> usersHeader = new ArrayList<>();
        usersHeader.add("id");
        usersHeader.add("city");
        usersHeader.add("country");
        ParallelDataset<Row> usersDs = LoaderManager.textLoaderManager("data/users/", usersHeader, true, Formats.CSV.getSplitter());
        users = usersDs
                .mapToPair(d -> {
                    Row d1 = (Row) d;
                    List<Object> values1 = d1.getValues();
                    Tuple2<Long, Row> longRowTuple2 = new Tuple2<>(Long.parseLong((String) values1.get(0)), d1);
                    values1.remove(0);
                    return longRowTuple2;
                });
    }

    @Test
    void select() {
        List<Tuple2> user_id = clicks.setHeader(new String[]{"date", "screen", "click_target"}).select("date").collect();
        Set collect = user_id.stream().map(d -> d.getValue()).collect(Collectors.toSet());
        Map<Object, Object> collect1 = user_id.stream().collect(Collectors.toMap(d -> d.getKey(), d -> d.getValue(), (d1, d2) -> d2));
        Assertions.assertEquals(11, clicks.count());
        Assertions.assertEquals(8, collect.size());
        Assertions.assertEquals(1, collect1.keySet().size());
        Assertions.assertEquals(1, collect1.size());
    }

    @Test
    void filter() {
        Assertions.assertEquals(2, clicks.filter(d -> d.getValues().get(0).equals("2017-12-15")).count());
    }

    @Test
    void collect() {
        Assertions.assertEquals(12, users.collect().size());
    }

    @Test
    void count() {
        Assertions.assertEquals(12, users.count());
    }
}