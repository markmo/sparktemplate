import diamond.io.{CSVSink, CSVSource}
import diamond.transformation.TransformationContext
import diamond.transformation.row.{AppendColumnRowTransformation, RowTransformation}
import diamond.transformation.table.RowTransformationPipeline
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Path
import scala.util.Try

/**
  * Created by markmo on 10/01/2016.
  */
object SparkApp extends App {

  val conf = new SparkConf().setAppName("sparktemplate").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // Sample Pipeline

  val source = CSVSource(sqlContext)
  val sink = CSVSink(sqlContext)

  val inPath = getClass.getResource("events_sample.csv").getPath
  val outPath = "/tmp/out"

  val inputSchema = StructType(
    StructField("entityIdType", StringType) ::
    StructField("entityId", StringType) ::
    StructField("attribute", StringType) ::
    StructField("ts", StringType) ::
    StructField("value", StringType, nullable = true) ::
    StructField("properties", StringType, nullable = true) ::
    StructField("processTime", StringType) :: Nil
  )

  val ctx = new TransformationContext

  ctx("in_path", inPath)
  ctx("out_path", outPath)
  ctx("schema", inputSchema)

  // aliases
  val transform = RowTransformation
  val append = AppendColumnRowTransformation

  // Transformations

  // Append a new column with the value "Hello" to each row
  val hello = append(
    name = "Hello",
    columnName = "test",
    dataType = StringType
  ) { (row, ctx) =>
    "Hello"
  }

  // Remove out path if exists
  val out = Path(outPath)
  Try(out.deleteRecursively())

  // Construct Pipeline
  val pipeline = new RowTransformationPipeline("template")
  pipeline.addTransformations(hello)

  // Run Pipeline
  val results = pipeline.run(source, sink, ctx)

  results.take(10).foreach(println)

  sc.stop()

}
