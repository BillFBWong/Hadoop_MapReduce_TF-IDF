# Hadoop_MapReduce_TF-IDF

基于MapReduce的TF-IDF统计

## Instruction

### Build jar file

Compile and build in `code` folder

```bash
javac code/TFIDF.java
jar cvf code/TFIDF.jar code/TFIDF*.class
```

### Run on Hadoop

```bash
hadoop jar code/TFIDF.jar TFIDF input output
```

### Result

Once job finished, result will be shown in file `output/part-r-00000`.

## Example

### input

```plain
---File1---
TITLE
firstline text text, text.
secondline text text.

---File2---
TITLE
firstline text text.
```

### output

```plain
firstline   0.0=(File1, 1),(File2, 1)
secondline  0.69=(File1,1)
text        0.0=(File1, 5), (File2, 2)
TITLE       0.0=(File1, 1), (File2, 1)
```

## Design

### Mapper

Split terms in every line. Filename is inclueded in key.

input: `offset => line`

output: `term,filename => 1`

### Combiner

Combine all record with same term and filename, add up term frequency.

input: `term,filename => 1`

output: `term => (filename, term_frequency)`

### Reducer

Merge values into a list then calculate IDF.

input: `term => (filename, term_frequency)`

output: `term => IDF = [(filename1, term_frequency1), (filename2, term_frequency2), ...]`
