import java.io._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

object BinaryToDataframe {

	val BIN_FILE_PATH = "/media/gogu/20e10f72-9e6f-46d8-9655-903936fdcbbe/junk/binary/data.bin"
	val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(BIN_FILE_PATH))))

	case class BinData(c1:Int,c2:Long,c3:String,c4:Int)

	def main (args: Array[String]) {
		this.generateBinaryFile()
		this.binaryToDf
	}

	private def generateBinaryFile() = {
		val dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(BIN_FILE_PATH))))
		val r = scala.util.Random
		for(i<- 1 to 20*1000*1000){
			dos.writeInt(r.nextInt())
			dos.writeLong(r.nextLong())
//			//Make a random alphanumeric string with size random between 1 and 30 chars
			dos.writeUTF(r.alphanumeric.take(r.nextInt(30)+1).mkString)
			dos.writeInt(1) //this is for control, to see if spark really reads all
		}
		dos.close()
	}

	def getBinData():BinData = {
		return BinData(
			dis.readInt(),
			dis.readLong(),
			dis.readUTF(),
			dis.readInt()
		)
	}

	private def binaryToDf = {

		val conf = new SparkConf().setAppName("Binary to dataframe").setMaster("local[1]")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)

		import sqlContext.implicits._


		val lazySeq = (1 to 15*1000*1000) //read 15 mil rows
		val rdd = sc.makeRDD(lazySeq).map(
			row =>
				{
					getBinData()
				}
		)
		val df = rdd.toDF()

		df.registerTempTable("data")
		sqlContext.sql("SELECT SUM(c4) FROM data").show()

	}

}
