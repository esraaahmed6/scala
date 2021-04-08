import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType};

package object common {
  val dataSchema = StructType(Array(
    StructField("Msisdn",IntegerType),
    StructField("Service_Identifier",IntegerType),
    StructField("Traffic_Volume",IntegerType),
    StructField("Time_stamp",StringType)



  ))
  val dataseg = StructType(Array(
    StructField("p_no",IntegerType)

  ))

  val datasrul = StructType(Array(
    StructField("app_id",IntegerType),
    StructField("app",IntegerType),
    StructField("start_date",IntegerType),
    StructField("end_date",IntegerType),
    StructField("vol_use",IntegerType)

  ))

}