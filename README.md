import org.apache.spark.sql.SparkSession
val spark: SparkSession = ...

import org.apache.spark.sql.DataFrame

// Using format-agnostic load operator
val csvs: DataFrame = spark
  .read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .load("*.csv")

// Using format-specific load operator
val jsons: DataFrame = spark
  .read
  .json("metrics/*.json")ON or JavaScript Object Notation is a popular file format for storing semi-structured data. Transforming it to a table is not always easy and sometimes downright ridiculous. However, Pandas offers the possibility via the read_json function. If you are not familiar with the orient argument, you might have a hard time.

First, let’s take a look at the read_json() documentation. In my opinion, one of the most valuable arguments that you can pass to this function is the orient argument, because it give an indication of how your JSON file is structured.

'split' : dict like {index -> [index], columns -> [columns], data -> [values]}
'records' : list like [{column -> value}, ... , {column -> value}]
'index' : dict like {index -> {column -> value}}
'columns' : dict like {column -> {index -> value}}
'values' : just the values array
When you want to read your JSON file or object into a Pandas object, you’re gonna have to find the right value for the orient argument. To help you understand these five formats, let’s go over all of them.

Read_json with split
In the following lines of code I created a JSON string that matches the split orientation. You can see that I specified three required keys: columns, index and data.

Python
1
json_split = json.dumps({
2
    "columns": ["weight","count"],
3
    "index": ["apples","bananas","cherries","pears"],
4
    "data": [[5,20],[8,30],[2,120],[4,16]]
5
})
6
pd.read_json(json_split, orient = 'split')
7
​
There’s not much flexibility here. If you change the keys (other than columns, index or data), you’ll run into the following error.

in check_keys_split
 raise ValueError(f"JSON data had unexpected key(s): {bad_keys}")
ValueError: JSON data had unexpected key(s)
If you add extra values to a particular row, you’ll be greeted with another error:

in _list_to_arrays
 raise ValueError(e) from e
ValueError: X columns passed, passed data had X columns
However, you can mix data types.

READ_JSON WITH records
When we use the split orientation, we assume that on every line, we specified a key and a value, with a key matching the column and — for each row — its value matching the values in that column.
