/*
 * Copyright 2017 M.Breuer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@SerialVersionUID(123456789L)
class SmartMatcher(pattern: scala.util.matching.Regex) extends Serializable {
  // fieldNames => list of field-names
  // nameToGroup => field-name to capture-group index
  val (fieldNames, nameToGroup) = {
    val REGEX_CAPTURE_GROUPS = "\\((\\?<([A-Za-z0-9]+)>)?".r
    val namedCaptureGroups = REGEX_CAPTURE_GROUPS
      .findAllMatchIn(pattern.toString)
      .zipWithIndex
      .filter(e => e._1.group(0).startsWith("(?<"))
      .toList
    (namedCaptureGroups.map(m => m._1.group(2)).toList,
      namedCaptureGroups.map(m => (m._1.group(2), m._2 + 1)).toMap)
  }

  println(" fieldNames: " + fieldNames)
  println(" nameToGroup: " + nameToGroup)

  def parse(text: String): List[String] = {
    val dict = nameToGroup
    val m = pattern.findFirstMatchIn(text)
    var result = {
      m match {
        case Some(n) => fieldNames.map(field => { n.group(dict.get(field).get) }).toList
        case _ => println("match failed for: " + text); null
      }
    }
    return result
  }
  
  def parseMap( text: String ) : Map[String,String] = {
    var values = parse(text).toArray;
    var result = fieldNames
      .zipWithIndex
      .map( x => (x._1 -> values.apply(x._2)))
      .toMap
      
    return result;
  }

  def schema(): org.apache.spark.sql.types.StructType = {
    val fields = fieldNames.map(field => org.apache.spark.sql.types.StructField(field, org.apache.spark.sql.types.StringType, true))
    return org.apache.spark.sql.types.StructType(fields)
  }

  def parseDataset(sparkSession: org.apache.spark.sql.SparkSession, dataSet: org.apache.spark.sql.Dataset[String]): org.apache.spark.sql.DataFrame = {
    val stringRDD = dataSet.rdd
    return parseRDD(sparkSession, stringRDD)
  }

  def parseRDD(sparkSession: org.apache.spark.sql.SparkSession, textRDD: org.apache.spark.rdd.RDD[String]): org.apache.spark.sql.DataFrame = {
    val rowRDD = textRDD.map { line => org.apache.spark.sql.Row.fromSeq(this.parse(line).toSeq) }
    return sparkSession.createDataFrame(rowRDD, this.schema())
  }

}
