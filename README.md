# Data Engineering Homework: implement map-reduce framework simulation

This assignment is somewhat artificial, because we do not code our own map-reduce frameworks at work, however it's a good excercise to test your understanding of software development, code composition and data management.

Sometimes data can't fit into a single machine memory and one of the solutions is to split it into smaller chunks and process it bit by bit. To speed things up one can use multiple machines to process the chunked data in parallel. This is what [Apache Hadoop MapReduce](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html), inspired by [Google's MapReduce whitepaper](http://static.googleusercontent.com/media/research.google.com/en/us/archive/mapreduce-osdi04.pdf), is all about:

> A MapReduce job usually splits the input data-set into independent chunks which are processed by the **map tasks** in a completely parallel manner. The framework sorts the outputs of the maps, which are then input to the **reduce tasks**. Typically both the input and the output of the job are stored in a file-system. The framework takes care of scheduling tasks, monitoring them and re-executes the failed tasks.

We'd like you to implement your own simplified map-reduce framework in a programming language of your choice:

- The framework should be able to execute generic map and reduce computations.
- No network support or failure resilience is required — just make it work on a single machine.
- Data has to be processed in parallel, e.g. by using threads.
- A dataset is a directory containing multiple files with records, e.g. `data/users` directory with `part-001.csv` and `part-002.csv` files is a "users" dataset.
- We value your time, therefore files can be in either CSV or any other format of your personal preference — whatever is easier to implement.
- We value simplicity over cleverness.

Please see attached datasets to play with.

## Task #1: use implemented map-reduce framework for aggregation

Having in mind `data/clicks` dataset with "date" column, count how many clicks there were for each date and write the results to `data/total_clicks` dataset with "date" and "count" columns.

Here's the example in Ruby (hopefully it's simple enough as a working example) of how we'd like to see the framework be used for the aggregation:

```ruby
  MapReduce(
    map: {
      'data/clicks' => lambda { |clicks|
        clicks.map { |click|
          {
            key: click['date'],
            value: click
          }
        }
      }
    },
    reduce: lambda { |key, values|
      [
        {
          'date' => key,
          'count' => values.count
        }
      ]
    },
    output: 'data/clicks_per_day'
  )
```

## Task #2: join two datasets using implemented map-reduce framework

There are two datasets:

- `data/users` dataset with columns "id" and "country"
- `data/clicks` dataset with columns "date", "user_id" and "click_target"

We'd like to produce a new dataset called `data/filtered_clicks` that includes only those clicks that belong to users from Lithuania (`country=LT`).

Here's the example of how we'd like to see the framework be used for the join:

```ruby
  MapReduce(
    map: {
      'data/users' => lambda { |users|
        users
          .select { |user| user['country'] == 'LT' }
          .map { |user|
            {
              key: user['id'],
              value: user.merge('table' => 'users')
            }
          }
      },
      'data/clicks' => lambda { |clicks|
        clicks.map { |click|
          {
            key: click['user_id'],
            value: click.merge('table' => 'clicks')
          }
        }
      }
    },
    reduce: lambda { |key, values|
      user = values.select { |value| value['table'] == 'users' }.first

      values
        .select { |value| value['table'] == 'clicks' }
        .map { |click| click.merge(user || {}) }
        .select { |click| !click['country'].nil? }
    },
    output: 'data/filtered_clicks'
  )
```

## Notes

Please note that computations in these tasks need to be performed using your own framework, do not use an existing solution such as Apache MapReduce or Spark.

Also consider that all actions should be able to execute in a distributed fassion (multithreaded in the case of this excercise), including data reading and writing operations. This may not always be possible, but do your best :)

Along with the solution we'd like to get an instruction on how to run these simulations just to make sure everything works as expected :) Afterwards we'll invite you to discuss the solution. If you need something clarified — please don't hesitate to ask and we wish you good luck!