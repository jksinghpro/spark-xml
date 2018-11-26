# XML Data Source for Apache Spark(Added Support for xsd(XML schema definition) )

### Note: This project is an extension of spark xml to support xsd 

- A library for parsing and querying XML data with Apache Spark, for Spark SQL and DataFrames.
The structure and test tools are mostly copied from [XML Data Source for Spark](https://github.com/databricks/spark-xml).

- This package supports to process format-free XML files in a distributed way, unlike JSON datasource in Spark restricts in-line JSON format.

- Apart  from the original version , i have added a support for specifying xsd in the option so as to validate xmln's against and it and parse the xmln's to dataframes.


## Requirements

This library requires Spark 2.0+ for 0.4.x.

For version that works with Spark 1.x, please check for [branch-0.3](https://github.com/databricks/spark-xml/tree/branch-0.3).


## Usage 

 Clone this repo and build this project

```
git clone https://github.com/jksinghpro/spark-xml.git
```


## Features
This package allows reading XML files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.6.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `xsd`: XML definition of the xml data. Either the path of xsd file can be specified or xsd as a string can also be specified.Same option works for both.
* `path`: Location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `rowTag`: The row tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`. Default is `ROW`. At the moment, rows containing self closing xml tags are not supported.
* `samplingRatio`: Sampling ratio for inferring schema (0.0 ~ 1). Default is 1. Possible types are `StructType`, `ArrayType`, `StringType`, `LongType`, `DoubleType`, `BooleanType`, `TimestampType` and `NullType`, unless user provides a schema for this.
* `excludeAttribute` : Whether you want to exclude attributes in elements or not. Default is false.
* `treatEmptyValuesAsNulls` : (DEPRECATED: use `nullValue` set to `""`) Whether you want to treat whitespaces as a null value. Default is false
* `mode`: The mode for dealing with corrupt records during parsing. Default is `PERMISSIVE`.
  * `PERMISSIVE` : sets other fields to `null` when it meets a corrupted record, and puts the malformed string into a new field configured by `columnNameOfCorruptRecord`. When a schema is set by user, it sets `null` for extra fields.
  * `DROPMALFORMED` : ignores the whole corrupted records.
  * `FAILFAST` : throws an exception when it meets corrupted records.
* `columnNameOfCorruptRecord`: The name of new field where malformed strings are stored. Default is `_corrupt_record`.
* `attributePrefix`: The prefix for attributes so that we can differentiate attributes and elements. This will be the prefix for field names. Default is `_`.
* `valueTag`: The tag used for the value when there are attributes in the element having no child. Default is `_VALUE`.
* `charset`: Defaults to 'UTF-8' but can be set to other valid charset names
* `ignoreSurroundingSpaces`: Defines whether or not surrounding whitespaces from values being read should be skipped. Default is false.

When writing files the API accepts several options:
* `path`: Location to write files.
* `rowTag`: The row tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`. Default is `ROW`.
* `rootTag`: The root tag of your xml files to treat as the root. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `books`. Default is `ROWS`.
* `nullValue`: The value to write `null` value. Default is string `null`. When this is `null`, it does not write attributes and elements for fields.
* `attributePrefix`: The prefix for attributes so that we can differentiating attributes and elements. This will be the prefix for field names. Default is `_`.
* `valueTag`: The tag used for the value when there are attributes in the element having no child. Default is `_VALUE`.
* `compression`: compression codec to use when saving to file. Should be the fully qualified name of a class implementing `org.apache.hadoop.io.compress.CompressionCodec` or one of case-insensitive shorten names (`bzip2`, `gzip`, `lz4`, and `snappy`). Defaults to no compression when a codec is not specified.

Currently it supports the shortened name usage. You can use just `xml` instead of `com.databricks.spark.xml` from Spark 1.5.0+

## Structure Conversion

Due to the structure differences between `DataFrame` and XML, there are some conversion rules from XML data to `DataFrame` and from `DataFrame` to XML data. Note that handling attributes can be disabled with the option `excludeAttribute`.




### Conversion from XML to `DataFrame`

- __Attributes__: Attributes are converted as fields with the heading prefix, `attributePrefix`.

    ```xml
    ...
    <one myOneAttrib="AAAA">
        <two>two</two>
        <three>three</three>
    </one>
    ...
    ```
    produces a schema below:

    ```
    root
     |-- _myOneAttrib: string (nullable = true)
     |-- two: string (nullable = true)
     |-- three: string (nullable = true)
    ```

- __Value in an element that has no child elements but attributes__: The value is put in a separate field, `valueTag`.

    ```xml
    ...
    <one>
        <two myTwoAttrib="BBBBB">two</two>
        <three>three</three>
    </one>
    ...
    ```
    produces a schema below:
    ```
    root
     |-- two: struct (nullable = true)
     |    |-- _VALUE: string (nullable = true)
     |    |-- _myTwoAttrib: string (nullable = true)
     |-- three: string (nullable = true)
    ```

### Conversion from `DataFrame` to XML

- __Element as an array in an array__:  Writing a XML file from `DataFrame` having a field `ArrayType` with its element as `ArrayType` would have an additional nested field for the element. This would not happen in reading and writing XML data but writing a `DataFrame` read from other sources. Therefore, roundtrip in reading and writing XML files has the same structure but writing a `DataFrame` read from other sources is possible to have a different structure.

    `DataFrame` with a schema below:
    ```
     |-- a: array (nullable = true)
     |    |-- element: array (containsNull = true)
     |    |    |-- element: string (containsNull = true)
    ```

    with data below:
    ```
    +------------------------------------+
    |                                   a|
    +------------------------------------+
    |[WrappedArray(aa), WrappedArray(bb)]|
    +------------------------------------+
    ```

    produces a XML file below:
    ```xml
    ...
    <a>
        <item>aa</item>
    </a>
    <a>
        <item>bb</item>
    </a>
    ...
    ```


## Examples

These examples use a XML file available for download [here](https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml):

```
$ wget https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml
```

### SQL API

XML data source for Spark can infer data types:
```sql
CREATE TABLE books
USING com.databricks.spark.xml
OPTIONS (path "books.xml", rowTag "book")
```

You can also specify column names and types in DDL. In this case, we do not infer schema.
```sql
CREATE TABLE books (author string, description string, genre string, _id string, price double, publish_date string, title string)
USING com.databricks.spark.xml
OPTIONS (path "books.xml", rowTag "book")
```

### Scala API

#### Using XSD

```scala
import org.apache.spark.sql.SQLContext


val sqlContext = new SQLContext(sc)
val df = sqlContext.read
  .format("com.databricks.spark.xml")
  .option("rowTag", "book")
  .option("xsd","books.xsd")
  .load("books.xml")

val df = sqlContext.read
  .format("com.databricks.spark.xml")
  .option("rowTag", "book")
  .option("xsd","<xs:schema attributeFormDefault="unqualified" elementFormDefault="qualified" xmlns:xs="http://www.w3.org/2001/XMLSchema">
                         <xs:element name="catalog">
                             <xs:complexType>
                                 <xs:sequence>
                                     <xs:element name="book" maxOccurs="unbounded" minOccurs="0">
                                         <xs:complexType>
                                             <xs:sequence>
                                                 <xs:element type="xs:string" name="author"/>
                                                 <xs:element type="xs:string" name="title"/>
                                                 <xs:element type="xs:string" name="genre"/>
                                                 <xs:element type="xs:double" name="price"/>
                                                 <xs:element type="xs:date" name="publish_date"/>
                                                 <xs:element type="xs:string" name="description"/>
                                             </xs:sequence>
                                             <xs:attribute type="xs:string" name="id" use="optional"/>
                                         </xs:complexType>
                                     </xs:element>
                                 </xs:sequence>
                             </xs:complexType>
                         </xs:element>
                     </xs:schema>")
  .load("books.xml"
  
```

#### Without XSD
```scala
import org.apache.spark.sql.SQLContext
import com.databricks.spark.xml._

val sqlContext = new SQLContext(sc)
val df = sqlContext.read
  .option("rowTag", "book")
  .xml("books.xml")
    
  
val selectedData = df.select("author", "_id")
selectedData.write
  .option("rootTag", "books")
  .option("rowTag", "book")
  .xml("newbooks.xml")
```

Alternatively you can specify the format to use instead:

```scala
import org.apache.spark.sql.SQLContext


val sqlContext = new SQLContext(sc)
val df = sqlContext.read
  .format("com.databricks.spark.xml")
  .option("rowTag", "book")
  .load("books.xml")

val selectedData = df.select("author", "_id")
selectedData.write
  .format("com.databricks.spark.xml")
  .option("rootTag", "books")
  .option("rowTag", "book")
  .save("newbooks.xml")
```

You can manually specify the schema when reading data:

```scala
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};

val sqlContext = new SQLContext(sc)
val customSchema = StructType(Array(
  StructField("_id", StringType, nullable = true),
  StructField("author", StringType, nullable = true),
  StructField("description", StringType, nullable = true),
  StructField("genre", StringType ,nullable = true),
  StructField("price", DoubleType, nullable = true),
  StructField("publish_date", StringType, nullable = true),
  StructField("title", StringType, nullable = true)))


val df = sqlContext.read
  .format("com.databricks.spark.xml")
  .option("rowTag", "book")
  .schema(customSchema)
  .load("books.xml")

val selectedData = df.select("author", "_id")
selectedData.write
  .format("com.databricks.spark.xml")
  .option("rootTag", "books")
  .option("rowTag", "book")
  .save("newbooks.xml")
```

### Java API

```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);
DataFrame df = sqlContext.read()
  .format("com.databricks.spark.xml")
  .option("rowTag", "book")
  .load("books.xml");

df.select("author", "_id").write()
  .format("com.databricks.spark.xml")
  .option("rootTag", "books")
  .option("rowTag", "book")
  .save("newbooks.xml");
```

You can manually specify schema:
```java
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

SQLContext sqlContext = new SQLContext(sc);
StructType customSchema = new StructType(new StructField[] {
  new StructField("_id", DataTypes.StringType, true, Metadata.empty()),
  new StructField("author", DataTypes.StringType, true, Metadata.empty()),
  new StructField("description", DataTypes.StringType, true, Metadata.empty()),
  new StructField("genre", DataTypes.StringType, true, Metadata.empty()),
  new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
  new StructField("publish_date", DataTypes.StringType, true, Metadata.empty()),
  new StructField("title", DataTypes.StringType, true, Metadata.empty())
});

DataFrame df = sqlContext.read()
  .format("com.databricks.spark.xml")
  .option("rowTag", "book")
  .schema(customSchema)
  .load("books.xml");

df.select("author", "_id").write()
  .format("com.databricks.spark.xml")
  .option("rootTag", "books")
  .option("rowTag", "book")
  .save("newbooks.xml");
```

### Python API

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='book').load('books.xml')
df.select("author", "_id").write \
    .format('com.databricks.spark.xml') \
    .options(rowTag='book', rootTag='books') \
    .save('newbooks.xml')
```

You can manually specify schema:
```python
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)
customSchema = StructType([ \
    StructField("_id", StringType(), True), \
    StructField("author", StringType(), True), \
    StructField("description", StringType(), True), \
    StructField("genre", StringType(), True), \
    StructField("price", DoubleType(), True), \
    StructField("publish_date", StringType(), True), \
    StructField("title", StringType(), True)])

df = sqlContext.read \
    .format('com.databricks.spark.xml') \
    .options(rowTag='book') \
    .load('books.xml', schema = customSchema)

df.select("author", "_id").write \
    .format('com.databricks.spark.xml') \
    .options(rowTag='book', rootTag='books') \
    .save('newbooks.xml')
```

### R API

Automatically infer schema (data types)
```R
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-xml_2.10:0.4.1" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)

df <- read.df(sqlContext, "books.xml", source = "com.databricks.spark.xml", rowTag = "book")

# In this case, `rootTag` is set to "ROWS" and `rowTag` is set to "ROW".
write.df(df, "newbooks.csv", "com.databricks.spark.xml", "overwrite")
```

You can manually specify schema:
```R
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:0.4.1" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)
customSchema <- structType(
    structField("_id", "string"),
    structField("author", "string"),
    structField("description", "string"),
    structField("genre", "string"),
    structField("price", "double"),
    structField("publish_date", "string"),
    structField("title", "string"))

df <- read.df(sqlContext, "books.xml", source = "com.databricks.spark.xml", rowTag = "book")

# In this case, `rootTag` is set to "ROWS" and `rowTag` is set to "ROW".
write.df(df, "newbooks.csv", "com.databricks.spark.xml", "overwrite")
```

## Hadoop InputFormat

The library contains a Hadoop input format for reading XML files by a start tag and an end tag. This is similar with [XmlInputFormat.java](https://github.com/apache/mahout/blob/9d14053c80a1244bdf7157ab02748a492ae9868a/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java) in [Mahout](http://mahout.apache.org) but supports to read compressed files, different encodings and read elements including attributes,
which you may make direct use of as follows:

```scala
import com.databricks.spark.xml.XmlInputFormat

// This will detect the tags including attributes
sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<book>")
sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</book>")
sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")

val records = sc.newAPIHadoopFile(
  path,
  classOf[XmlInputFormat],
  classOf[LongWritable],
  classOf[Text])
```

#ISSUES

- Issue Tracker: https://github.com/jksinghpro/spark-xml/issues

