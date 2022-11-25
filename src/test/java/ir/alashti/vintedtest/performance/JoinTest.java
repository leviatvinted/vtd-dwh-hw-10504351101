package ir.alashti.vintedtest.performance;

import com.google.common.base.Stopwatch;
import ir.alashti.vintedtest.dataset.Formats;
import ir.alashti.vintedtest.dataset.Row;
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
import java.util.concurrent.TimeUnit;

class JoinTest {
    static ParallelPairDataset<Long, Row> left;
    static ParallelPairDataset<Long, Row> right;

    @BeforeAll
    static void setUp() {
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
        left = users
                .filter(d -> ((String) ((Row) d).getValues().get(2)).equalsIgnoreCase("LT"))
                .mapToPair(d -> {
                    Row d1 = (Row) d;
                    List<Object> values1 = d1.getValues();
                    Tuple2<Long, Row> longRowTuple2 = new Tuple2<>(Long.parseLong((String) values1.get(0)), d1);
                    values1.remove(0);
                    return longRowTuple2;
                }).setHeader(new String[]{"id", "city", "country"});
        right = clicks.mapToPair(d -> {
            Row d1 = (Row) d;
            List<Object> values = d1.getValues();
            Tuple2<Long, Row> longRowTuple2 = new Tuple2<>(Long.parseLong((String) values.get(2)), d1);
            values.remove(2);
            return longRowTuple2;
        }).setHeader(new String[]{"date", "screen", "user_id", "click_target"});
    }

    @Test
    public void join() {
        ParallelDataset<Tuple3<Long, Row>> join = null;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < 1000000; i++) {
            join = left.join(right);
        }
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        Assertions.assertEquals(39, join.count());
    }
}
