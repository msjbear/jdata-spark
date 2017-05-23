package com.sjmei.jdata.utils

import java.io.StringWriter
import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine

/**
  * Created by meishangjian on 2017/05/10.
  */
object VelocityUtils {

  def genDimFeatureSql(templatePath:String, table_suffix:String, initDate: String): String = {

    val velocityEngine = new VelocityEngine();
    val context = new VelocityContext();
    context.put("table_suffix", table_suffix);
    context.put("initDate", initDate);

    val sw = new StringWriter();
    velocityEngine.mergeTemplate(templatePath, "utf-8", context, sw);
    println(sw.toString())

    sw.toString
  }

  def plusOneDay(dateString: String): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(dateString)
    val cal: Calendar = new GregorianCalendar()
    cal.setTime(date)
    cal.add(Calendar.DAY_OF_YEAR, 1)

    sdf.format(cal.getTime())
  }


  def main(args: Array[String]): Unit = {

    genDimFeatureSql("data/dim_feature/feature_wide_table.sql","part1","2016-04-01")

    println(plusOneDay("2016-04-01")>"2016-05-04")
  }

}
