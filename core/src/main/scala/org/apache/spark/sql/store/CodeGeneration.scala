/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.store

import java.sql.PreparedStatement
import java.util.Collections

import scala.collection.JavaConverters._
import scala.util.hashing.MurmurHash3

import com.gemstone.gemfire.internal.InternalDataSerializer
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdHeapDataOutputStream
import org.codehaus.janino.CompilerFactory

import org.apache.spark.Logging
import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator, CodegenContext, ExprCode, GeneratedClass}
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.UncompressedEncoder
import org.apache.spark.sql.execution.columnar.{ColumnWriter, ExternalStoreUtils}
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Utilities to generate code for exchanging data from Spark layer
 * (Row, InternalRow) to store (Statement, ExecRow).
 * <p>
 * This extends the Spark code generation facilities to allow lazy
 * generation of code string itself only if not found in cache
 * (and using some other lookup key than the code string)
 */
object CodeGeneration extends Logging {

  /**
   * A loading cache of generated <code>GeneratedStatement</code>s.
   */
  private[this] val cache = CacheBuilder.newBuilder().maximumSize(100).build(
    new CacheLoader[ExecuteKey, GeneratedStatement]() {
      override def load(key: ExecuteKey): GeneratedStatement = {
        val start = System.nanoTime()
        val result = compilePreparedUpdate(key.name, key.schema, key.dialect)
        val elapsed = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"PreparedUpdate expression code generated in $elapsed ms")
        result
      }
    })

  /**
   * Similar to Spark's CodeGenerator.compile cache but allows lookup using
   * a key (name+schema) instead of the code string itself to avoid having
   * to create the code string upfront. Code adapted from CodeGenerator.cache
   */
  private[this] val codeCache = CacheBuilder.newBuilder().maximumSize(100).build(
    new CacheLoader[ExecuteKey, (GeneratedClass, Array[Any])]() {
      // invoke CodeGenerator.doCompile by reflection to reduce code duplication
      private val doCompileMethod = {
        val allMethods = CodeGenerator.getClass.getDeclaredMethods.toSeq
        val method = allMethods.find(_.getName.endsWith("doCompile"))
            .getOrElse(sys.error(s"Failed to find method 'doCompile' in " +
                s"CodeGenerator (methods=$allMethods)"))
        method.setAccessible(true)
        method
      }

      override def load(key: ExecuteKey): (GeneratedClass, Array[Any]) = {
        val (code, references) = key.genCode()
        val startTime = System.nanoTime()
        val result = doCompileMethod.invoke(CodeGenerator, code)
        val endTime = System.nanoTime()
        val timeMs = (endTime - startTime).toDouble / 1000000.0
        CodegenMetrics.METRIC_SOURCE_CODE_SIZE.update(code.body.length)
        CodegenMetrics.METRIC_COMPILATION_TIME.update(timeMs.toLong)
        logInfo(s"Local code for ${key.name} generated in $timeMs ms")
        (result.asInstanceOf[GeneratedClass], references)
      }
    })

  private[this] val indexCache = CacheBuilder.newBuilder().maximumSize(100).build(
    new CacheLoader[ExecuteKey, GeneratedIndexStatement]() {
      override def load(key: ExecuteKey): GeneratedIndexStatement = {
        val start = System.nanoTime()
        val result = compileGeneratedIndexUpdate(key.name, key.schema, key.dialect)
        val elapsed = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"PreparedUpdate expression code generated in $elapsed ms")
        result
      }
    })

  /**
   * A loading cache of generated <code>SerializeComplexType</code>s.
   */
  private[this] val typeCache = CacheBuilder.newBuilder().maximumSize(100).build(
    new CacheLoader[DataType, SerializeComplexType]() {
      override def load(key: DataType): SerializeComplexType = {
        val start = System.nanoTime()
        val result = compileComplexType(key)
        val elapsed = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"Serializer code generated in $elapsed ms")
        result
      }
    })

  private[this] def getColumnSetterFragment(col: Int, dataType: DataType,
      dialect: JdbcDialect, ev: ExprCode, stmt: String, schema: String,
      ctx: CodegenContext): String = {
    val timeUtilsClass = DateTimeUtils.getClass.getName.replace("$", "")
    val encoderClass = classOf[UncompressedEncoder].getName
    val nonNullCode = dataType match {
      case IntegerType => s"$stmt.setInt(${col + 1}, ${ev.value});"
      case LongType => s"$stmt.setLong(${col + 1}, ${ev.value});"
      case DoubleType => s"$stmt.setDouble(${col + 1}, ${ev.value});"
      case FloatType => s"$stmt.setFloat(${col + 1}, ${ev.value});"
      case ShortType => s"$stmt.setInt(${col + 1}, ${ev.value});"
      case ByteType => s"$stmt.setInt(${col + 1}, ${ev.value});"
      case BooleanType => s"$stmt.setBoolean(${col + 1}, ${ev.value});"
      case StringType => s"$stmt.setString(${col + 1}, ${ev.value}.toString());"
      case BinaryType => s"$stmt.setBytes(${col + 1}, ${ev.value});"
      case TimestampType =>
        s"$stmt.setTimestamp(${col + 1}, $timeUtilsClass.toJavaTimestamp(${ev.value}));"
      case DateType =>
        s"$stmt.setDate(${col + 1}, $timeUtilsClass.toJavaDate(${ev.value}));"
      case _: DecimalType =>
        s"$stmt.setBigDecimal(${col + 1}, ${ev.value}.toJavaBigDecimal());"
      case a: ArrayType =>
        val encoderVar = ctx.freshName("encoderObj")
        val arr = ctx.freshName("arr")
        val encoder = ctx.freshName("encoder")
        val cursor = ctx.freshName("cursor")
        ctx.addMutableState(encoderClass, encoderVar,
          s"$encoderVar = new $encoderClass();")
        s"""
           |final ArrayData $arr = ${ev.value};
           |final $encoderClass $encoder = $encoderVar;
           |long $cursor = $encoder.initialize($schema[$col], 1, false);
           |${ColumnWriter.genCodeArrayWrite(ctx, a, encoder, cursor,
              arr, "0")}
           |// finish and set the bytes into the statement
           |$stmt.setBytes(${col + 1}, $encoder.finish($cursor).array());
        """.stripMargin
      case m: MapType =>
        val encoderVar = ctx.freshName("encoderObj")
        val map = ctx.freshName("mapValue")
        val encoder = ctx.freshName("encoder")
        val cursor = ctx.freshName("cursor")
        ctx.addMutableState(encoderClass, encoderVar,
          s"$encoderVar = new $encoderClass();")
        s"""
           |final MapData $map = ${ev.value};
           |final $encoderClass $encoder = $encoderVar;
           |long $cursor = $encoder.initialize($schema[$col], 1, false);
           |${ColumnWriter.genCodeMapWrite(ctx, m, encoder, cursor, map, "0")}
           |// finish and set the bytes into the statement
           |$stmt.setBytes(${col + 1}, $encoder.finish($cursor).array());
        """.stripMargin
      case s: StructType =>
        val encoderVar = ctx.freshName("encoderObj")
        val struct = ctx.freshName("structValue")
        val encoder = ctx.freshName("encoder")
        val cursor = ctx.freshName("cursor")
        ctx.addMutableState(encoderClass, encoderVar,
          s"$encoderVar = new $encoderClass();")
        s"""
           |final InternalRow $struct = ${ev.value};
           |final $encoderClass $encoder = $encoderVar;
           |long $cursor = $encoder.initialize($schema[$col], 1, false);
           |${ColumnWriter.genCodeStructWrite(ctx, s, encoder, cursor,
              struct, "0")}
           |// finish and set the bytes into the statement
           |$stmt.setBytes(${col + 1}, $encoder.finish($cursor).array());
        """.stripMargin
      case _ =>
        s"$stmt.setObject(${col + 1}, ${ev.value});"
    }
    val code = if (ev.code == "") ""
    else {
      val c = s"${ev.code}\n"
      ev.code = ""
      c
    }
    val jdbcType = ExternalStoreUtils.getJDBCType(dialect, NullType)
    s"""
       |${code}if (${ev.isNull}) {
       |  $stmt.setNull(${col + 1}, $jdbcType);
       |} else {
       |  $nonNullCode
       |}
    """.stripMargin
  }

  private[this] def defaultImports = Array(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[MapData].getName,
      classOf[MutableRow].getName)

  def getRowSetterFragment(schema: Array[StructField],
      dialect: JdbcDialect, row: String, stmt: String,
      schemaTerm: String, ctx: CodegenContext): String = {
    val rowInput = (col: Int) => ExprCode("", s"$row.isNullAt($col)",
      ctx.getValue(row, schema(col).dataType, Integer.toString(col)))
    genStmtSetters(schema, dialect, rowInput, stmt, schemaTerm, ctx)
  }

  def genStmtSetters(schema: Array[StructField], dialect: JdbcDialect,
      rowInput: Int => ExprCode, stmt: String, schemaTerm: String,
      ctx: CodegenContext): String = {
    schema.indices.map { col =>
      getColumnSetterFragment(col, schema(col).dataType, dialect,
        rowInput(col), stmt, schemaTerm, ctx)
    }.mkString("")
  }

  private[this] def compilePreparedUpdate(table: String,
      schema: Array[StructField], dialect: JdbcDialect): GeneratedStatement = {
    val ctx = new CodegenContext
    val stmt = ctx.freshName("stmt")
    val multipleRows = ctx.freshName("multipleRows")
    val rows = ctx.freshName("rows")
    val batchSize = ctx.freshName("batchSize")
    val schemaTerm = ctx.freshName("schema")
    val row = ctx.freshName("row")
    val rowCount = ctx.freshName("rowCount")
    val result = ctx.freshName("result")
    val code = getRowSetterFragment(schema, dialect, row, stmt, schemaTerm, ctx)

    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedEvaluation")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(defaultImports)
    val separator = "\n      "
    val varDeclarations = ctx.mutableStates.map { case (javaType, name, init) =>
      s"$javaType $name;$separator${init.replace("this.", "")}"
    }
    val expression = s"""
      ${varDeclarations.mkString(separator)}
      int $rowCount = 0;
      int $result = 0;
      while ($rows.hasNext()) {
        InternalRow $row = (InternalRow)$rows.next();
        $code
        $rowCount++;
        if ($multipleRows) {
          $stmt.addBatch();
          if (($rowCount % $batchSize) == 0) {
            $result += $stmt.executeBatch().length;
            $rowCount = 0;
          }
        }
      }
      if ($multipleRows) {
        if ($rowCount > 0) {
          $result += $stmt.executeBatch().length;
        }
      } else {
        $result += $stmt.executeUpdate();
      }
      return $result;
    """

    logDebug(s"DEBUG: For update to table=$table, generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[GeneratedStatement],
      Array(stmt, multipleRows, rows, batchSize, schemaTerm))
        .asInstanceOf[GeneratedStatement]
  }

  private[this] def compileGeneratedIndexUpdate(table: String,
      schema: Array[StructField], dialect: JdbcDialect): GeneratedIndexStatement = {
    val ctx = new CodegenContext
    val schemaTerm = ctx.freshName("schema")
    val stmt = ctx.freshName("stmt")
    val row = ctx.freshName("row")
    val code = getRowSetterFragment(schema, dialect, row, stmt, schemaTerm, ctx)

    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedIndexEvaluation")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(defaultImports)
    val separator = "\n      "
    val varDeclarations = ctx.mutableStates.map { case (javaType, name, init) =>
      s"$javaType $name;$separator${init.replace("this.", "")}"
    }
    val expression = s"""
      ${varDeclarations.mkString(separator)}
        $code
        stmt.addBatch();
      return 1;"""

    logDebug(s"DEBUG: For update to index=$table, generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[GeneratedIndexStatement],
      Array(schemaTerm, stmt, row)).asInstanceOf[GeneratedIndexStatement]
  }

  private[this] def compileComplexType(
      dataType: DataType): SerializeComplexType = {
    val ctx = new CodegenContext
    val inputVar = ctx.freshName("value")
    val encoderVar = ctx.freshName("encoder")
    val fieldVar = ctx.freshName("field")
    val dosVar = ctx.freshName("dos")
    val typeConversion = dataType match {
      case a: ArrayType =>
        val arr = ctx.freshName("arr")
        val cursor = ctx.freshName("cursor")
        s"""
           |final ArrayData $arr = (ArrayData)$inputVar;
           |long $cursor = $encoderVar.initialize($fieldVar, 1, false);
           |${ColumnWriter.genCodeArrayWrite(ctx, a, encoderVar, cursor,
              arr, "0")}
           |if ($dosVar != null) {
           |  final byte[] b = $encoderVar.finish($cursor).array();
           |  InternalDataSerializer.writeByteArray(b, b.length, $dosVar);
           |  return null;
           |} else {
           |  return $encoderVar.finish($cursor).array();
           |}
        """.stripMargin
      case m: MapType =>
        val map = ctx.freshName("mapValue")
        val cursor = ctx.freshName("cursor")
        s"""
           |final MapData $map = (MapData)$inputVar;
           |long $cursor = $encoderVar.initialize($fieldVar, 1, false);
           |${ColumnWriter.genCodeMapWrite(ctx, m, encoderVar, cursor,
              map, "0")}
           |if ($dosVar != null) {
           |  final byte[] b = $encoderVar.finish($cursor).array();
           |  InternalDataSerializer.writeByteArray(b, b.length, $dosVar);
           |  return null;
           |} else {
           |  return $encoderVar.finish($cursor).array();
           |}
        """.stripMargin
      case s: StructType =>
        val struct = ctx.freshName("structValue")
        val cursor = ctx.freshName("cursor")
        s"""
           |final InternalRow $struct = (InternalRow)$inputVar;
           |long $cursor = $encoderVar.initialize($fieldVar, 1, false);
           |${ColumnWriter.genCodeStructWrite(ctx, s, encoderVar, cursor,
              struct, "0")}
           |if ($dosVar != null) {
           |  final byte[] b = $encoderVar.finish($cursor).array();
           |  InternalDataSerializer.writeByteArray(b, b.length, $dosVar);
           |  return null;
           |} else {
           |  return $encoderVar.finish($cursor).array();
           |}
        """.stripMargin
      case _ => throw Utils.analysisException(
        s"complex type conversion: unexpected type $dataType")
    }

    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedSerialization")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(Array(classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[MapData].getName,
      classOf[InternalDataSerializer].getName,
      classOf[MutableRow].getName))
    val separator = "\n      "
    val varDeclarations = ctx.mutableStates.map { case (javaType, name, init) =>
      s"$javaType $name;$separator${init.replace("this.", "")}"
    }
    val expression = s"""
      ${varDeclarations.mkString(separator)}
      $typeConversion"""

    logDebug(s"DEBUG: For complex type=$dataType, generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[SerializeComplexType],
      Array(inputVar, encoderVar, fieldVar, dosVar))
        .asInstanceOf[SerializeComplexType]
  }

  private[this] def executeUpdate(name: String, stmt: PreparedStatement,
      rows: java.util.Iterator[InternalRow], multipleRows: Boolean,
      batchSize: Int, schema: Array[StructField], dialect: JdbcDialect): Int = {
    val result = cache.get(new ExecuteKey(name, schema, dialect))
    result.executeStatement(stmt, multipleRows, rows, batchSize, schema)
  }

  def executeUpdate(name: String, stmt: PreparedStatement,
      rows: Iterator[InternalRow], multipleRows: Boolean, batchSize: Int,
      schema: Array[StructField], dialect: JdbcDialect): Int =
    executeUpdate(name, stmt, rows.asJava, multipleRows, batchSize,
      schema, dialect)

  def executeUpdate(name: String, stmt: PreparedStatement, rows: Seq[Row],
      multipleRows: Boolean, batchSize: Int, schema: Array[StructField],
      dialect: JdbcDialect): Int = {
    val iterator = new java.util.Iterator[InternalRow] {

      private val baseIterator = rows.iterator
      private val encoder = RowEncoder(StructType(schema))

      override def hasNext: Boolean = baseIterator.hasNext

      override def next(): InternalRow = {
        encoder.toRow(baseIterator.next())
      }

      override def remove(): Unit =
        throw new UnsupportedOperationException("remove not supported")
    }
    executeUpdate(name, stmt, iterator, multipleRows, batchSize,
      schema, dialect)
  }

  def executeUpdate(name: String, stmt: PreparedStatement, row: Row,
      schema: Array[StructField], dialect: JdbcDialect): Int = {
    val encoder = RowEncoder(StructType(schema))
    executeUpdate(name, stmt, Collections.singleton(encoder.toRow(row))
        .iterator(), multipleRows = false, 0, schema, dialect)
  }

  def compileCode(name: String, schema: Array[StructField],
      genCode: () => (CodeAndComment, Array[Any])): (GeneratedClass,
      Array[Any]) = {
    codeCache.get(new ExecuteKey(name, schema, GemFireXDDialect,
      forIndex = false, genCode = genCode))
  }

  def getComplexTypeSerializer(dataType: DataType): SerializeComplexType =
    typeCache.get(dataType)

  def getGeneratedIndexStatement(name: String, schema: StructType,
      dialect: JdbcDialect): (PreparedStatement, InternalRow) => Int = {
    val result = indexCache.get(new ExecuteKey(name, schema.fields,
      dialect, forIndex = true))
    result.addBatch(schema.fields)
  }

  def removeCache(name: String): Unit =
    cache.invalidate(new ExecuteKey(name, null, null))

  def removeCache(dataType: DataType): Unit = cache.invalidate(dataType)

  def removeIndexCache(indexName: String): Unit =
    indexCache.invalidate(new ExecuteKey(indexName, null, null, true))

  def clearAllCache(skipTypeCache: Boolean = true): Unit = {
    cache.invalidateAll()
    codeCache.invalidateAll()
    indexCache.invalidateAll()
    if (!skipTypeCache) {
      typeCache.invalidateAll()
    }
  }
}

trait GeneratedStatement {

  @throws[java.sql.SQLException]
  def executeStatement(stmt: PreparedStatement, multipleRows: Boolean,
      rows: java.util.Iterator[InternalRow], batchSize: Int,
      schema: Array[StructField]): Int
}

trait SerializeComplexType {

  @throws[java.io.IOException]
  def serialize(value: Any, encoder: UncompressedEncoder,
      field: StructField, dos: GfxdHeapDataOutputStream): Array[Byte]
}

trait GeneratedIndexStatement {

  @throws[java.sql.SQLException]
  def addBatch(schema: Array[StructField])
      (stmt: PreparedStatement, row: InternalRow): Int
}


final class ExecuteKey(val name: String,
    val schema: Array[StructField], val dialect: JdbcDialect,
    val forIndex: Boolean = false,
    val genCode: () => (CodeAndComment, Array[Any]) = null) {

  override lazy val hashCode: Int = if (schema != null && !forIndex) {
    MurmurHash3.listHash(name :: schema.toList, MurmurHash3.seqSeed)
  } else name.hashCode

  override def equals(other: Any): Boolean = other match {
    case o: ExecuteKey => if (schema != null && o.schema != null && !forIndex) {
      val numFields = schema.length
      if (numFields == o.schema.length && name == o.name) {
        var i = 0
        while (i < numFields) {
          if (!schema(i).equals(o.schema(i))) {
            return false
          }
          i += 1
        }
        true
      } else false
    } else {
      name == o.name
    }
    case s: String => name == s
    case _ => false
  }
}
