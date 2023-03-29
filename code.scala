import java.io.FileWriter
import java.io.File
import java.lang.Thread.sleep
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.{FileSystems, Path, Paths, WatchService}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import scala.collection.mutable.Queue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object run1 extends App {

  var num_of_rows: Int=0

  // path of Log file
  val logFilePath: Path = Paths.get("F:\\ibrahim\\iti\\Scala\\target\\rules_engine.log")

  // path of source directory
  val sourceDirPath: Path = Paths.get("F:\\ibrahim\\iti\\Scala\\source_dir")

  val queue: Queue[Path] = Queue.empty  // queue for holding files that will be processed

  // watch service to watch source directory and process files will be pasted in it
  val watchService: WatchService = FileSystems.getDefault.newWatchService()
  sourceDirPath.register(watchService, ENTRY_CREATE)

  // this loop keeps program run along, reload watch service with new changes in the source dir
  while (true) {
    val watchKey = watchService.poll(1, TimeUnit.SECONDS)
    if (watchKey != null) {
      val watchEvents = watchKey.pollEvents()
      watchEvents.forEach { event =>
        val fileName = event.context().asInstanceOf[Path].getFileName().toString()
        val filePath = sourceDirPath.resolve(fileName)
        queue.enqueue(filePath)  // adding file to the queue to be processed
      }
      watchKey.reset()
    }

    // Process files in queue
    while (queue.nonEmpty) {
      val filePath = queue.dequeue()
      val fileName = filePath.getFileName().toString()
      // future for every file to make program run as async
      val future = Future {
        processFile(filePath.toString) // calling processing method
      }

      // check future result, and depending on the result, there will be an action
      future.onComplete {
        case Success(_) =>
          // writing in log file
          val fileWriter = new FileWriter(logFilePath.toFile(), true)
          fileWriter.write(System.currentTimeMillis()+":success:file "+fileName.toString+ " with number of rows : "
            +num_of_rows+ " processed successfully\n")
          println(s"File $fileName processed successfully")
          fileWriter.close()
          //deleting file after processing
          new File(filePath.toString).delete()
        case Failure(ex) => println(s"Failed to process file $fileName: ${ex}")
      }
    }
    sleep(1000)
  }

  def processFile(file: String): Unit = {

    // DB connection params
    val url = "jdbc:oracle:thin:@localhost:1521:xe"
    val username = "hr"
    val password = "hr"
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val conn:Connection = DriverManager.getConnection(url, username, password)


    // take input and get data as a list of String
    val source_file = io.Source.fromFile(file)
    val source = source_file.getLines().drop(1).map(_.split(",").toList).toList

    // for getting rows num to add it to log file after processing
    num_of_rows= source.length

    // how processing will work ?
    // ok, we have 6 rules and we will keep six discount values for every order all of them initialized with zero value
    // if the rule will be applied on an order, the discount value will be changes otherwise will remain 0
    // and this will help in get top 2 discounts at the end

    // Qualifying Rule No1:
    // depending on the remaining days of expiry date of the product, this function parsing data and compare order data with expiry date
    // and depending on the difference, the result wil be.
    def ThirtyDaysDiscounts(data: List[String]): List[String] = {
      val timestamp = data.head
      val dateStr = data(2)

      // Formatters for date & timestamp
      val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

      // Applying formatters to data
      val timestampDate = LocalDate.parse(timestamp, timestampFormatter)
      val date = LocalDate.parse(dateStr, dateFormatter)
      val difference = ChronoUnit.DAYS.between(timestampDate, date)

      // getting disc value based on a condition
      val discount = if (30 - difference.toInt < 0) {
        0
      } else {
        (30 - difference.toInt) * 1
      }
      // return of function
      data :+ discount.toString
    }

    // Qualifying Rule No2:
    // As required: if it's Cheese products there is 10% discount and if it's wine there is 5% discount
    //we have applied case match on product column to check type of product and add discount based on it's type
    def WineOrCheeseDiscount(list: List[String]): List[String] = {
      val product = list(1)
      val discount = product match {
        case str if str.contains("Wine") => "5"
        case str if str.contains("Cheese") => "10"
        case _ => "0"
      }
      list :+ discount.toString
    }

    // Qualifying Rule No3:
    // A clear one, if it's 23 march, order will have 50% discount
    def March23Discount(list: List[String]): List[String] = {
      val timestamp = list.head
      if (timestamp.startsWith("2023-03-23")) {
        list :+ "50"
      }
      else {
        list :+ "0"
      }
    }

    // Qualifying Rule No4:
    // Depending on quantity of a product in the order, discounts will be applied
    // as per ranges : 6 – 9 units ‐> 5% discount
    //10‐14 units ‐> 7% discount
    //More than 15 ‐> 10% discount
    def MoreThanFiveDiscount(list: List[String]): List[String] = {
      val quantity = list(3).toInt // get product quantity
      // applying rule
      val quantity_discount = quantity match {
        case str if quantity > 5 && quantity < 10 => "5"
        case str if quantity >= 10 && quantity < 15 => "7"
        case str if quantity >= 15 => "10"
        case _ => "0"
      }
      list :+ quantity_discount.toString
    }

    // Qualifying Rule No5:
    // We want to make a rule of special discount for people using App Channel and this discount is rounded to the nearest multiple of 5 ,so we are going to get the value of discount
    // First of all, we will filter to get the columns of  channel and quantity of the product bought
    // For product bought through App we are going to get the % of discount by division of the quantity by 5
    // then ceil the value to the nearest integer then multiply with 0.05 , otherwise we are going to give value of 0
    def applyAppDiscounts(list: List[String]): List[String] = {
      val channel = list(5)
      val quantity = list(3).toInt
      val discountPercentage = if (channel.toLowerCase().equals("app")) {
        (quantity / 5).ceil * 5
      } else {
        0
      }

      list :+ discountPercentage.toInt.toString
    }

    // Qualifying Rule No6:
    // For Payment Method of Visa we are going to make special discount which is 5%,
    // so we are going to filter to get the column of payment Method and then check who use visa
    // and then apply the rule of 5% otherwise will be 0 discount
    def applyVisaDiscounts(list: List[String]): List[String] = {
      val paymentType = list(6)
      val discountPercentage = if (paymentType.toLowerCase().equals("visa")) {
        5
      } else {
        0
      }
      list :+ discountPercentage.toString
    }

    // in case we have more than 2 rules applied on the same order we need to take top 2 discounts and get average of them
    // we have 6 values for 6 rules in every row regardless rule is applied or not
    // using sorting and taking first 2 values will achieve the goal then sum them and divide by 2 to get the final average discount value
    def Top2DiscountsAvg(list: List[String]): List[String] = {
      val Rulesvals = list.slice(list.length - 6, list.length).map(_.toInt).sortWith(_ > _).take(2).sum.toDouble / 2.0
      list.take(7) :+ Rulesvals.toString
    }

    // Final Price calculator
    def calcPrice(list: List[String]): List[String] = {
      // get the old price by multiplying quantity*price
      val total_before_disc = list(3).toString.toDouble * list(4).toString.toDouble
      // get the new/final price by subtract the discount from old price
      val total_price_after = total_before_disc - (total_before_disc * list(7).toString.toDouble / 100)
      list :+ total_price_after.toString
    }

    // Function for inserting data in Oracle database
    def insertData(row: List[String]): Unit = {

      var stmt: PreparedStatement = null

      try {

        stmt = conn.prepareStatement("INSERT INTO discounts (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount, price) VALUES (TO_TIMESTAMP_TZ(?, 'YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM'), ?, ?, ?, ?, ?, ?, ?, ?)")

        val timestamp = row(0)
        val productName = row(1)
        val expiryDate = java.sql.Date.valueOf(row(2))
        val quantity = row(3).toInt
        val unitPrice = row(4).toDouble
        val channel = row(5)
        val paymentMethod = row(6)
        val discount = row(7).toDouble
        val price = row(8).toDouble

        stmt.setString(1, timestamp)
        stmt.setString(2, productName)
        stmt.setDate(3, expiryDate)
        stmt.setInt(4, quantity)
        stmt.setDouble(5, unitPrice)
        stmt.setString(6, channel)
        stmt.setString(7, paymentMethod)
        stmt.setDouble(8, discount)
        stmt.setDouble(9, price)

        stmt.executeUpdate()

      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (stmt != null) stmt.close()
      }

    }

    val z =source.map(ThirtyDaysDiscounts)
      .map(WineOrCheeseDiscount)
      .map(March23Discount)
      .map(MoreThanFiveDiscount)
      .map(applyAppDiscounts)
      .map(applyVisaDiscounts)
      .map(Top2DiscountsAvg)
      .map(calcPrice)
//      .map(insertData)
        print(z)
    if (conn != null) conn.close()
    if (source_file != null) source_file.close()


  }
}
