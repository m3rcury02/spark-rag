---
layout: global
title: Quick Start
displayTitle: Quick Start
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.
---

* This will become a table of contents (this text will be scraped).
{:toc}

This tutorial provides a quick introduction to using Spark. See the [programming guide](rdd-programming-guide.html)
for a more complete reference.

# Interactive Analysis with the Spark Shell

## Basics

Spark's shell provides a simple way to learn the API, as well as a powerful tool to analyze data interactively.

<div class="codetabs">

<div data-lang="python" markdown="1">

Spark's primary abstraction is a distributed collection of items called a Dataset. As a result, all Datasets
in Python are Dataset[Row], and we call it `DataFrame` to be consistent with the data frame concept in Pandas.

{% highlight python %}
>>> textFile = spark.read.text("README.md")
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">

{% highlight scala %}
scala> val textFile = spark.read.textFile("README.md")
textFile: org.apache.spark.sql.Dataset[String] = [value: string]
{% endhighlight %}

</div>

</div>

## Spark Properties

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.app.name</code></td>
  <td>(none)</td>
  <td>
    The name of your application. This will appear in the UI and in log data.
  </td>
  <td>0.9.0</td>
</tr>
</table>

## TF-IDF

<div class="d-none">
This div is only here to make the IDE happy.
</div>

Refer to the [`HashingTF` Python docs](api/python/reference/api/pyspark.ml.feature.HashingTF.html) for details.

{% include_example python/ml/tf_idf_example.py %}

{% include_api_gen generated-agg-funcs-table.html %}
