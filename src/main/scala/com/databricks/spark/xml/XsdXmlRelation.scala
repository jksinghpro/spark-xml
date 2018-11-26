package com.databricks.spark.xml

import java.io.File

import com.databricks.spark.avro.SchemaConverters
import com.databricks.spark.xml.avro.{DatumBuilder, SchemaBuilder}
import com.databricks.spark.xml.parsers.StaxXmlParser
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.StructType
/**
  * Created by exa00015 on 26/11/18.
  */
class XsdXmlRelation(override val baseRDD: () => RDD[String],
                     override val location: Option[String],
                     override val parameters: Map[String, String],
                     override val userSchema: StructType = null)
                    (@transient override val sqlContext: SQLContext)
  extends XmlRelation(baseRDD,
  location,
  parameters,
  userSchema )(sqlContext){

  val xsd = parameters.getOrElse("xsd", throw new IllegalArgumentException("XSD not specified") )

  override def buildScan(): RDD[Row] = {
    var sparkSchema: StructType = null
    val schemaBuilder = new SchemaBuilder()
    if(new File(xsd).exists()){
      val avroRdd = baseRDD().map(record => {
        val datumBuilder = new DatumBuilder(new SchemaBuilder().createSchema(new File(xsd)))
        datumBuilder.createDatum(new String(record)).toString
      })
      val sparkSchema = SchemaConverters.toSqlType(
        new Schema.Parser().parse(schemaBuilder.createSchema(new File(xsd)).toString))
        .dataType.asInstanceOf[StructType]
      sqlContext.read.schema(sparkSchema).json(avroRdd).rdd
    } else {
      val avroRdd = baseRDD().map(record => {
        val datumBuilder = new DatumBuilder(new SchemaBuilder().createSchema(xsd))
        datumBuilder.createDatum(new String(record)).toString
      })
      val sparkSchema = SchemaConverters.toSqlType(
        new Schema.Parser().parse(schemaBuilder.createSchema(xsd).toString))
        .dataType.asInstanceOf[StructType]
      sqlContext.read.schema(sparkSchema).json(avroRdd).rdd
    }

  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val requiredFields = requiredColumns.map(schema(_))
    val schemaFields = schema.fields
    if (schemaFields.deep == requiredFields.deep) {
      buildScan()
    } else {
      val requestedSchema = StructType(requiredFields)
      StaxXmlParser.parse(
        baseRDD(),
        requestedSchema,
        options)
    }
  }




}
