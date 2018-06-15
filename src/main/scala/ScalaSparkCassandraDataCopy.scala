package ScalaSparkCassandra

import com.typesafe.config.ConfigFactory
import org.apache.spark._
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector._
import java.util._
import java.sql.Timestamp
import com.datastax.driver.core.utils.UUIDs
import scala.xml._
import com.datastax.spark.connector.cql.CassandraConnector
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.{ List => JList }
import scala.collection.immutable.List

object ScalaSparkCassandraDataCopy {

  def main(args: Array[String]) {

    def cassandraFrom = "127.0.0.1"
    def cassandraTo = "127.0.0.1"

    val conf = new SparkConf().setAppName("ScalaSparkCassandraDataCopy")
      .set("spark.cassandra.connection.host", cassandraFrom)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val connectorFromCluster = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", cassandraFrom))
    val connectorToCluster = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", cassandraTo))

    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    val timediffFormat = new java.text.SimpleDateFormat("HH'hrs' mm'min' ss's'");
    val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = UUIDs.startOf(0).timestamp();

    def getTimeFormated(thru: Long) = { timediffFormat.format(new Date().getTime + 18000000L - thru) }
    def getTimeFromUUID(uuid: UUID) = { if (uuid == null) { 0 } else (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000L }
    def cassandraRowToMap(cr: CassandraRow) = { cr.columnNames.zip(cr.columnValues).toMap }
    def dateDiff(thru: Date) = thru.getTime() - new Date().getTime()
    def dateValid(thru: Date) = (dateDiff(thru) + 7970408640L) / 1000
    def dateExpired(thru: Date) = if (dateDiff(thru) < 0 && (thru.getTime() + 7970408640L + 86400000) < new Date().getTime()) { 10 } else { dateValid(thru) + 86400 }
    def dateVerifyForTTL(oldRow: CassandraRow, column: String) = if (oldRow.get[Option[Date]](column) == None) { 0 } else { if (oldRow.get[Date](column).getTime() > new Date().getTime()) { 0 } else { if (dateDiff(oldRow.get[Date](column)) < 0 && dateValid(oldRow.get[Date](column)) < 0) { 10 } else { dateValid(oldRow.get[Date](column)) } } }
    def verifyBoolColumnSetFalse(oldRow: CassandraRow, column: String) = if (oldRow.get[Option[Boolean]](column) == None) { false } else { oldRow.getBoolean(column) }
    def verifyTimeStampColumn(oldRow: CassandraRow, column: String) = if (oldRow.get[Option[Date]](column) == None) { 0 } else { oldRow.getDate(column).getTime() }
    def verifyUUIDColumn(oldRow: CassandraRow, column: String) = if (oldRow.get[Option[UUID]](column) == None) { null } else { oldRow.getUUID(column) }
    def verifyIntColumn(oldRow: CassandraRow, column: String) = if (oldRow.get[Option[Int]](column) == None) { 0 } else { oldRow.getInt(column) }
    def verifyLongColumn(oldRow: CassandraRow, column: String) = if (oldRow.get[Option[Long]](column) == None) { 0L } else { oldRow.getLong(column) }
    def verifyStringTimeStampColumn(oldRow: CassandraRow, column: String) = if (oldRow.get[Option[String]](column) == None) { null } else { oldRow.getDate(column).getTime() }
    def verifyStringColumn(oldRow: CassandraRow, column: String) = { if (oldRow.get[Option[String]](column) == None || oldRow.getString(column).trim().length() == 0) { null } else { oldRow.getString(column) } }
    def verifyStringColumnWithReplace(oldRow: CassandraRow, column: String) = { if (oldRow.get[Option[String]](column) == None || oldRow.getString(column).trim().length() == 0) { null } else { oldRow.getString(column).replaceAll("\"", "\\\\\"") } }  
    def verifyUUIDTSColumn(oldRow: CassandraRow, column: String) = if (oldRow.get[Option[Date]](column) == None) { null } else { UUIDs.startOf(oldRow.getDate(column).getTime()) }
    def verifyStringToDate(oldRow: CassandraRow, column: String) = if (oldRow.get[Option[String]](column) == None || oldRow.getString(column) == "") { null } else { oldRow.getDate(column).getTime() }
    def SumValues(a: Long, b: Long) = a + b + 1

    val keyspaces = List("keyspace")

    for (keyspace <- keyspaces) {
      var tables = {
        implicit val c = connectorToCluster
        sc.cassandraTable("system", "schema_columns")
          .select("columnfamily_name")
          .where("keyspace_name= '" + keyspace + "'").distinct().collect();
      }

      for (tb <- tables) {

        var tableName = tb.getString("columnfamily_name")
        System.out.println(tableName);

        val orig = tableName
        val dest = tableName;

        val origCT = {
          implicit val c = connectorFromCluster
          sc.cassandraTable(keyspace, orig)
        }
        val destCT = {
          implicit val c = connectorToCluster
          sc.cassandraTable(keyspace, dest)
        }
        val copy = origCT;
        {
          implicit val c = connectorToCluster
          origCT.saveToCassandra(keyspace, dest);
        }
      }
    }
    System.out.println("END ----------------------------------------");
    sc.stop()
    sys.exit(0)
  }
}