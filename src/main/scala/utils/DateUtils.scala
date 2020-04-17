package utils

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, ZoneId}
import java.util.{Calendar, Date}

object DateUtils {

  def timestamp2Date(timeMillis: Long, outputFormat: String): String ={
    //println(timeMillis)
    new SimpleDateFormat(outputFormat).format(new Date(timeMillis))
  }

  def dateFormat(date: String, inputFormat: String, outputFormat: String) : String = {
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new SimpleDateFormat(inputFormat).parse(date))
    new SimpleDateFormat(outputFormat).format(dateAux.getTime())
  }

  def dateFormat(date: Date, outputFormat: String) : String = {
    new SimpleDateFormat(outputFormat).format(date)
  }

  def dateAddAndFormat(date: String, days: Int, inputFormat: String, outputFormat: String) : String = {
    val dateAux = Calendar.getInstance()
    dateAux.setTime(new SimpleDateFormat(inputFormat).parse(date))
    dateAux.add(Calendar.DATE, days)
    new SimpleDateFormat(outputFormat).format(dateAux.getTime())
  }

  /**
    * return with the same format.
    * @param date
    * @param days
    * @param inputFormat
    * @return
    */
  def dateAddAndFormat(date: String, days: Int, inputFormat: String = "yyyyMMdd") : String = {
    dateAddAndFormat(date,days,inputFormat,inputFormat)
  }

  def dateDiff(start: String, end: String, dateFormat: String = "yyyyMMdd"): Int ={
    val startDate = Calendar.getInstance()
    val endDate = Calendar.getInstance()
    startDate.setTime(new SimpleDateFormat(dateFormat).parse(start))
    endDate.setTime(new SimpleDateFormat(dateFormat).parse(end))
    ChronoUnit.DAYS.between(
      LocalDate.of(startDate.get(Calendar.YEAR), startDate.get(Calendar.MONTH)+1, startDate.get(Calendar.DAY_OF_MONTH)),
      LocalDate.of(endDate.get(Calendar.YEAR), endDate.get(Calendar.MONTH)+1, endDate.get(Calendar.DAY_OF_MONTH))
    ).toInt
  }

  def dateAdd(date: Date, days: Int) : Date = {
    val dateAux = Calendar.getInstance()
    dateAux.setTime(date)
    dateAux.add(Calendar.DATE, days)
    dateAux.getTime()
  }


  def getAllDate(startDay: String, endDay: String): collection.mutable.ListBuffer[String] = {
    val startDate = Calendar.getInstance()
    startDate.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(startDay))
    val endDate = Calendar.getInstance()
    endDate.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(endDay))

    val dateList = collection.mutable.ListBuffer[String]()
    while (endDate.compareTo(startDate) >=0 ){
      dateList.append(new SimpleDateFormat("yyyy-MM-dd").format(startDate.getTime))
      startDate.add(Calendar.DATE, 1)
    }

    dateList
  }

  def str2Date(date: String, zoneId: ZoneId = ZoneId.of("Asia/Shanghai")): Date = {
    Date.from(LocalDate.parse(date).atStartOfDay().atZone(zoneId).toInstant)
  }

  /**
   * 获取月份最后一天
   * @return
   */
  def getMonthEnd(date: Date, format: String = "yyyy-MM-dd"):String={
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE,-1)
    new SimpleDateFormat(format).format(cal.getTime()) //该日期的月份最后一天
  }

  /**
   * 获取date所在月份的第一天
   * @param date
   * @param format
   * @return
   */
  def getMonthStart(date: Date, format: String = "yyyy-MM-dd"):String={
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.set(Calendar.DATE, 1)
    new SimpleDateFormat(format).format(cal.getTime()) //该日期的月份最后一天
  }


  def getChinaWeekDay(date: Date, weekDay: Int, format: String = "yyyy-MM-dd"): String = {
    val cal:Calendar =Calendar.getInstance()
    cal.setTime(date)
    if (cal.get(Calendar.DAY_OF_WEEK) != weekDay){
      if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){ //传入星期天日期，计算非星期天的日期，则需要算前一个星期的
        cal.add(Calendar.WEEK_OF_YEAR, -1)
      } else if(weekDay == Calendar.SUNDAY){ //传入非星期天的日期，计算星期天日期，则需要计算后一个星期的
        cal.add(Calendar.WEEK_OF_YEAR, 1)
      }
      cal.set(Calendar.DAY_OF_WEEK, weekDay)
    }
    new SimpleDateFormat(format).format(cal.getTime())
  }


  def main(args: Array[String]): Unit = {
    println(dateDiff("20200101","20200107"))
    val start = "20200101"
    val end = "20200107"
    println(DateUtils.dateDiff(start,end)/2 + 1)
  }

}
