/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer}
// $example off$
import org.apache.spark.sql.SparkSession

object OneHotEncoderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("OneHotEncoderExample")
      .getOrCreate()

    // $example on$
    val df = spark.createDataFrame(Seq(
      (0, "a","1","h1"),
      (1, "b","2","h2"),
      (2, "c","3","h3"),
      (3, "c","3","h3"),
      (4, "c","3","h3"),
      (5, "c","3","h3"),
      (6, "a","4","h2"),
      (7, "a","2","h1"),
      (8, "c","5","h2"),
      (9, "c","5","h3"),
      (10, "c","5","h5"),
      (11, "c","5","h5"),
      (12, "c","5","h6"),
      (13, "c","5","h6")
    )).toDF("id", "attr1","attr2","attr3")

    val stringColumns = Array("attr1","attr2","attr3")
    val indexTransformers: Array[org.apache.spark.ml.PipelineStage] = stringColumns.map(
      cname => new StringIndexer()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_index")
    )
    val index_pipeline = new Pipeline().setStages(indexTransformers)
    val df_indexed = index_pipeline.fit(df).transform(df)

    //encoding columns
    val indexColumns  = df_indexed.columns.filter(x => x contains "index")
    val oneHotEncoders: Array[org.apache.spark.ml.PipelineStage] = indexColumns.map(
      cname => new OneHotEncoder()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_vec")
    )
    val one_hot_pipeline = new Pipeline().setStages(oneHotEncoders)
    val df_oneHot = one_hot_pipeline.fit(df_indexed).transform(df_indexed)


    val df_assembler = new VectorAssembler()
      .setInputCols(Array("attr1_index_vec", "attr2_index_vec", "attr3_index_vec","id"))
      .setOutputCol("assemble_features").transform(df_oneHot)

   val df_vecIndexer = new VectorIndexer()
     .setInputCol("assemble_features")
     .setOutputCol("features_vecIdx")
     .setMaxCategories(10)
     .fit(df_assembler).transform(df_assembler)
    df_vecIndexer.select("features_vecIdx").rdd.map(_.mkString("\t")).take(10).foreach(println)


    val indexed = new StringIndexer()
      .setInputCol("attr1")
      .setOutputCol("categoryIndex")
      .fit(df).transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")

    val encoded = encoder.transform(indexed)
    encoded.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
