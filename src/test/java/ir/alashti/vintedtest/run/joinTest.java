package ir.alashti.vintedtest.run;

import ir.alashti.vintedtest.dataset.*;
import ir.alashti.vintedtest.dataset.input.LoaderManager;
import ir.alashti.vintedtest.dataset.output.WriterManager;
import ir.alashti.vintedtest.dataset.parallel.ParallelDataset;
import ir.alashti.vintedtest.dataset.parallel.ParallelPairDataset;
import ir.alashti.vintedtest.dataset.udf.Tuple2;
import ir.alashti.vintedtest.dataset.udf.Tuple3;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class joinTest {

    @BeforeAll
    static void setUp() {
    }

    @Test
    public void join() {
        List<String> clicksHeader = new ArrayList<>();
        clicksHeader.add("date");
        clicksHeader.add("screen");
        clicksHeader.add("user_id");
        clicksHeader.add("click_target");
        ParallelDataset<Row> clicks = LoaderManager.textLoaderManager("data/clicks", clicksHeader, true, Formats.CSV.getSplitter());

        List<String> usersHeader = new ArrayList<>();
        usersHeader.add("id");
        usersHeader.add("city");
        usersHeader.add("country");
        ParallelDataset<Row> users = LoaderManager.textLoaderManager("data/users", usersHeader, true, Formats.CSV.getSplitter());
        ParallelPairDataset<Long, Row> left = users
                .filter(d -> ((String) ((Row) d).getValues().get(2)).equalsIgnoreCase("LT"))
                .mapToPair(d -> {
                    Row d1 = (Row) d;
                    List<Object> values1 = d1.getValues();
                    Tuple2<Long, Row> longRowTuple2 = new Tuple2<>(Long.parseLong((String) values1.get(0)), d1);
                    values1.remove(0);
                    return longRowTuple2;
                }).setHeader(new String[]{"id", "city", "country"});
        ParallelPairDataset<Long, Row> right = clicks.mapToPair(d -> {
            Row d1 = (Row) d;
            List<Object> values = d1.getValues();
            Tuple2<Long, Row> longRowTuple2 = new Tuple2<>(Long.parseLong((String) values.get(2)), d1);
            values.remove(2);
            return longRowTuple2;
        }).setHeader(new String[]{"date", "screen", "user_id", "click_target"});
        ParallelDataset<Tuple3<Long, Row>> join = left.join(right);
        ParallelDataset<Tuple2> map = join.map(d -> {
            Tuple3<Long, Row> d1 = (Tuple3<Long, Row>) d;
            return new Tuple2<>(d1.getKey(), d1.getValue2());
        }).setHeader(new String[]{"id", "city", "country"});
        WriterManager.writerManager(map.collect(), "data/filtered_clicks.csv");
        Assertions.assertEquals(map.count(), 39);
    }

}