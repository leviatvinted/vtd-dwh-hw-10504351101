# MiniSpark Documentation

The overall process of this project's implementation is like below: 
1. designing and implementing the core asked functionalities single threaded. 
2. changing read, write, and operations to a multi-threaded form. 
3. adding tests
4. bugfix
5. clean code
6. documentation

The main mindset of designing this system is similar to Apache Spark. 
The following functionalities are implemented: 
- loading from csv files.
- map: transforms data but keeps the single column format. 
- mapToPair: transforms data to a key value datastructure. 
- filter: filters the data based on a predicate. 
- select: selects some specific columns from data. 
- reduce: reduces a PairDataset based on a Function. 
- join: joins two PairDataset based on their key matchings. only inner join is supported now. 
- collect: collects all the data to a flat data structure. 
- count: counts total number of data samples in a data structure. (Dataset, PairDataset)
- writing to csv files. 

The two `reduce` and `join` support data shuffling in the multi-threaded implementation. It was also tried to find the 
performant way of joining two sets based on their size in the runtime. 