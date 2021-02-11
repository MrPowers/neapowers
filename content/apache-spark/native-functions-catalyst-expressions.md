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



