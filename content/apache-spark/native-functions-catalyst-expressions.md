---
title: "Writing Spark Native Functions (Catalyst Expressions)"
date: 2021-02-10T08:38:38-05:00
draft: False
---

# Writing Spark Native Functions (Catalyst Expressions)

This post explains how Spark's public interface is exposed via catalyst expressions and how you can write your own functions in this manner.

Catalyst expressions are a great way to write performant code and learn about how Spark works under the hood.

## Walking backwards in the Spark codebase

The `org.apache.spark.sql.functions` object contains the following `add_months` method:

```scala
def add_months(startDate: Column, numMonths: Column): Column = withExpr {
  AddMonths(startDate.expr, numMonths.expr)
}
```

The IntelliJ text editor lets you easily navigate the source code with the Command + b shortcut.  Hover the mouse over the `AddMonths` class and press Command + b to see where it's defined in the source code.

Here's the class from the `org.apache.spark.sql.catalyst.expressions.datetimeExpressions` file.

```scala
case class AddMonths(startDate: Expression, numMonths: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, months: Any): Any = {
    DateTimeUtils.dateAddMonths(start.asInstanceOf[Int], months.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, m) => {
      s"""$dtu.dateAddMonths($sd, $m)"""
    })
  }

  override def prettyName: String = "add_months"
}
```

Let's keep digging and see how `DateTimeUtils.dateAddMonths` is defined.

Put your cursor over the `dateAddMonths` function invocation (in the `nullSafeEval` method) and press Command + b again.

You'll navigate to this function in `org.apache.spark.sql.catalyst.util.DateTimeUtils`:

```scala
def dateAddMonths(days: SQLDate, months: Int): SQLDate = {
  LocalDate.ofEpochDay(days).plusMonths(months).toEpochDay.toInt
}
```

## Defining your own Catalyst expression

Let's define a `bebe_beginning_of_month` function that returns the first day in a month.

Start by defining the function in `org.apache.spark.sql.BebeFunctions`:

```scala
object BebeFunctions {

  private def withExpr(expr: Expression): Column = Column(expr)

  def bebe_beginning_of_month(col: Column): Column = withExpr {
    BeginningOfMonth(col.expr)
  }

}
```

Define a `BeginningOfMonth` class in `org.apache.spark.sql.catalyst.expressions.BebeDatetimeExpressions`:

```scala
case class BeginningOfMonth(startDate: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def child: Expression = startDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def nullSafeEval(date: Any): Any = {
    BebeDateTimeUtils.getFirstDayOfMonth(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = BebeDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, sd => s"$dtu.getFirstDayOfMonth($sd)")
  }

  override def prettyName: String = "beginning_of_month"
}
```

Now define the `org.apache.spark.sql.catalyst.util.BebeDateTimeUtils.getFirstDayOfMonth` function:

```scala
package org.apache.spark.sql.catalyst.util

import java.time.LocalDate

object BebeDateTimeUtils {

  type SQLDate = Int

  /**
   * Returns first day of the month for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getFirstDayOfMonth(date: SQLDate): SQLDate = {
    val localDate = LocalDate.ofEpochDay(date)
    date - localDate.getDayOfMonth + 1
  }

}
```

Let's create a sample DataFrame and run the function to see it in action.

```scala
val df = Seq(
  (Date.valueOf("2020-01-15")),
  (Date.valueOf("2020-01-20")),
  (null)
).toDF("some_date")
  .withColumn("beginning_of_month", bebe_beginning_of_month(col("some_date")))

df.show()
```

```
+----------+------------------+
| some_date|beginning_of_month|
+----------+------------------+
|2020-01-15|        2020-01-01|
|2020-01-20|        2020-01-01|
|      null|              null|
+----------+------------------+
```

Run `df.explain(true)` to see the logical plans.

```
== Parsed Logical Plan ==
'Project [some_date#39, bebe_beginning_of_month('some_date) AS beginning_of_month#41]
+- Project [value#36 AS some_date#39]
   +- LocalRelation [value#36]

== Analyzed Logical Plan ==
some_date: date, beginning_of_month: date
Project [some_date#39, bebe_beginning_of_month(some_date#39) AS beginning_of_month#41]
+- Project [value#36 AS some_date#39]
   +- LocalRelation [value#36]

== Optimized Logical Plan ==
LocalRelation [some_date#39, beginning_of_month#41]

== Physical Plan ==
LocalTableScan [some_date#39, beginning_of_month#41]
```

Notice how Spark can "see" the `bebe_beginning_of_month` function that's in logical plans.  Spark can optimize catalyst expressions because they're visible.

UDFs on the other hand are [black boxes for Spark](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-udfs-blackbox.html) and should be avoided whenever possible.

## Catalyst expression libraries

The core Spark team is [hesitant to expose some Catalyst expressions to the Scala API](https://www.reddit.com/r/apachespark/comments/lcum8v/psa_the_spark_maintainers_are_intentionally/), so these functions will be exposed with the [bebe](https://github.com/MrPowers/bebe) project.

There is also an [itachi](https://github.com/yaooqinn/itachi) project that brings familiar functions from Postgres, Teradata, and Presto to Apache Spark.

[spark-alchemy](https://github.com/swoop-inc/spark-alchemy) provides an interface for registering Spark native functions and demonstrates how to build useful HyperLogLog native functions.  Sim has a [great blog post](https://blog.simeonov.com/2018/11/14/apache-spark-native-functions/) on Spark native functions if you're looking for more information.

## Next steps

Spark native functions often times need to be written in the `org.apache.spark.sql` namespace to bypass package privacy.

It's best to defined Spark native functions in a separate repo, so you're not mixing the Spark namespace with your application code.

Spark native functions are especially appropriate to fill in functionality gaps in the Spark API.  The Spark maintainers have a clear preference to keep the Spark API surface area small, so separate projects that add additional functionality are the path forward.

