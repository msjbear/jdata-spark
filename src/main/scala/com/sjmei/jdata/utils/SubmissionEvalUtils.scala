package com.sjmei.jdata.utils

import org.apache.spark.sql._

/**
  * Created by cdmeishangjian on 2017/5/7.
  */
object SubmissionEvalUtils {

  /**
    * evaluate model score
    *
    * @param predicts
    */
  def jdata_report(predicts: DataFrame, submission: DataFrame): Unit = {

    val all_label_user = predicts.select("user_id").where("label=1").distinct()
    val all_label_user_sku_pair = predicts.select("user_id", "sku_id").where("label=1").distinct()

    val all_predict_user = submission.select("user_id").distinct()
    val all_predict_user_sku_pair = submission.select("user_id", "sku_id").distinct()

    val pos_user_cnt = all_predict_user.join(all_label_user, Seq("user_id"), "inner").count()
    val all_predict_user_cnt = all_predict_user.count()
    val all_label_user_cnt = all_label_user.count()

    println(s"JDATA-pos_user_cnt:  $pos_user_cnt, " +
      s"all_predict_user_cnt: $all_predict_user_cnt, all_label_user_cnt: $all_label_user_cnt ")

    val all_user_acc = 1.0 * pos_user_cnt / Math.max(all_predict_user_cnt, 12000)
    val all_user_recall = 1.0 * pos_user_cnt / all_label_user_cnt

    val pos_user_sku_cnt = all_predict_user_sku_pair.
      join(all_label_user_sku_pair, Seq("user_id", "sku_id"), "inner").count()
    val all_predict_user_sku_cnt = all_predict_user_sku_pair.count()
    val all_label_user_sku_cnt = all_label_user_sku_pair.count()

    println(s"JDATA-pos_user_sku_cnt:  $pos_user_sku_cnt, " +
      s"all_predict_user_sku_cnt: $all_predict_user_sku_cnt, " +
      s"all_label_user_sku_cnt: $all_label_user_sku_cnt ")

    val all_user_sku_acc = 1.0 * pos_user_sku_cnt / Math.max(all_predict_user_sku_cnt, 12000)
    val all_user_sku_recall = 1.0 * pos_user_sku_cnt / all_label_user_sku_cnt

    println(s"JDATA-all_user_acc:  $all_user_acc, all_user_recall: $all_user_recall")
    println(s"JDATA-all_user_sku_acc:  $all_user_sku_acc, " +
      s"all_user_sku_recall: $all_user_sku_recall")

    val F11 = 6.0 * all_user_recall * all_user_acc / (5.0 * all_user_recall + all_user_acc)
    val F12 = 5.0 * all_user_sku_acc * all_user_sku_recall / (2.0 * all_user_sku_recall
      + 3 * all_user_sku_acc)

    val score = 4 * F11 + 6 * F12

    println(s"JDATA-final-score F11: $F11, F12: $F12, score: $score")

    all_label_user.unpersist(blocking = false)
    all_label_user_sku_pair.unpersist(blocking = false)
    all_predict_user.unpersist(blocking = false)
    all_predict_user_sku_pair.unpersist(blocking = false)
  }

  def jdata_user_report(predicts: DataFrame, submission: DataFrame): Unit = {

    val all_label_user = predicts.select("user_id").where("label=1").distinct()

    val all_predict_user = submission.select("user_id").distinct()

    val pos_user_cnt = all_predict_user.join(all_label_user, Seq("user_id"), "inner").count()
    val all_predict_user_cnt = all_predict_user.count()
    val all_label_user_cnt = all_label_user.count()

    println(s"JDATA-pos_user_cnt:  $pos_user_cnt, " +
      s"all_predict_user_cnt: $all_predict_user_cnt, all_label_user_cnt: $all_label_user_cnt ")

    val all_user_acc = 1.0 * pos_user_cnt / Math.max(all_predict_user_cnt, 12000)
    val all_user_recall = 1.0 * pos_user_cnt / all_label_user_cnt

    println(s"JDATA-all_user_acc:  $all_user_acc, all_user_recall: $all_user_recall")

    val F11 = 6.0 * all_user_recall * all_user_acc / (5.0 * all_user_recall + all_user_acc)

    val score = F11

    println(s"JDATA-final-score F11: $F11, score: $score")

    all_label_user.unpersist(blocking = false)
    all_predict_user.unpersist(blocking = false)
  }

}
