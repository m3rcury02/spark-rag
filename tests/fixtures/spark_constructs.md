---
layout: global
title: Constructs
displayTitle: Constructs found on real pages
---

Shortened from Spark docs pages (v3.5.0-v4.0.1): one of each construct the cleaner and chunker must handle.
Non-ASCII text stays as it is: café, naïve, “quoted”, Ā, 中文, and a dash — like this.

## Layout table

<table width="100%">
    <td>
{% highlight bash %}
# TERMINAL 1:
# Running Netcat

$ nc -lk 9999
apache spark
{% endhighlight %}
    </td>
    <td>
<div class="codetabs">
<div data-lang="python" markdown="1">
{% highlight bash %}
# TERMINAL 2: RUNNING structured_network_wordcount.py
$ ./bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py localhost 9999
{% endhighlight %}
</div>
</div>
    </td>
</table>

## Spans and nesting

<table>
<thead><tr><th>Left Input</th><th>Right Input</th><th>Join Type</th><th></th></tr></thead>
  <tr><td rowspan="2">Stream</td><td rowspan="2">Static</td><td>Inner</td><td>Supported, not stateful</td></tr>
  <tr><td>Right Outer</td><td>Not supported</td></tr>
  <tr><td colspan="2">Other queries</td><td>All types</td><td><ul><li>Append mode.</li><li>Update mode.</li></ul></td></tr>
  <tr><td>ArrayType</td><td><table><tr><th>Element type</th><th>PG Array</th></tr><tr><td>IntegerType</td><td>integer[]</td></tr></table></td><td>array</td><td></td></tr>
</table>

## Code in cells

<table>
  <thead><tr><th><b>Property Name</b></th><th><b>Meaning</b></th></tr></thead>
  <tr><td><code>rowTag</code></td><td>For example, in this xml: <code><xmp><books><book></book>...</books></xmp></code> the value would be book.</td></tr>
  <tr><td><code>cov</code> in <code>package:stats</code></td><td><code><pre>stats::cov(x, y = NULL, use = "everything",
           method = c("pearson", "kendall", "spearman"))</pre></code></td></tr>
  <tr><td><code>samplingRatio</code></td><td>Defines fraction of rows used for schema inferring.</td></tr>
</table>

## Pipe tables

|Keyword|Spark SQL<br/>ANSI Mode|Spark SQL<br/>NonANSI Mode|SQL-2016|
|:------|:-----:|-------:|----|
|ADD|non-reserved|non-reserved|non-reserved|
|**explode_outer** <br> ( *expr* )|reserved|non-reserved|reserved|

## Footnotes, rules and styles

MLlib uses linear algebra packages for optimised numerical processing[^1].

[^1]: To learn more about the benefits and background of system optimised natives, you may wish to
    watch Sam Halliday's ScalaX talk on [High Performance Linear Algebra in Scala](http://fommil.github.io/scalax14/#/).

***************************************************************************************************

<style type="text/css">
.tsclass {font-size:12px;color:#333333;width:40%;}
</style>
**<u>TABLE: person</u>** with <kbd>Ctrl</kbd> and <small>small print</small>.
{:.no_toc}

![Stream as a Table](img/structured-streaming-stream-as-a-table.png "Stream as a Table")
See [Advanced Dependency Management](https://spark.apache
.org/docs/latest/submitting-applications.html) and
["Improve Apache Spark write performance"](https://aws.amazon.com/blogs/big-data/improve-apache-spark-write-performance/
for details.

## Borders and preformatted text

{% highlight sql %}
SELECT * FROM person WHERE name LIKE 'M%';
+---+------+---+
| id|  name|age|
+---+------+---+
|300|  Mike| 80|
+---+------+---+
{% endhighlight %}

<pre>
ssh &lt;driver-node&gt; jstack &lt;pid&gt;
</pre>

## A long wrapped paragraph

Spark's scheduler is fully thread-safe and supports this use case to enable applications that serve
multiple requests (e.g. queries for multiple users). By default, Spark's scheduler runs jobs in FIFO
fashion. Each job is divided into "stages" (e.g. map and reduce phases), and the first job gets
priority on all available resources while its stages have tasks to launch, then the second job gets
priority, etc. If the jobs at the head of the queue don't need to use the whole cluster, later jobs
can start to run right away, but if the jobs at the head of the queue are large, then later jobs may be
delayed significantly. `spark.scheduler.mode` can be set to FAIR. (This is useful for multi-user
settings.) Starting in Spark 0.8, it is also possible to configure fair sharing between jobs. Under
fair sharing, Spark assigns tasks between jobs in a "round robin" fashion, so that all jobs get a
roughly equal share of cluster resources. This means that short jobs submitted while a long job is
running can start receiving resources right away and still get good response times, without waiting
for the long job to finish. This mode is best for multi-user settings.

- `spark.scheduler.allocation.file`: the path of the pool file, which can be local or remote.
- `spark.scheduler.pool`: the pool a thread's jobs go to; each thread can set its own local property.
- `spark.scheduler.mode`: FIFO or FAIR; FAIR shares the cluster between jobs in round-robin fashion.

{% include_example csv_dataset python/sql/datasource.py %}
