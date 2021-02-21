import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object testing {
  def main(args: Array[String]): Unit ={
    val sc = SparkSession.builder.appName("HITSImplementation")
      .master("local")
      .getOrCreate()
    val string="eclipse"
    val indexedtitle = sc.read.textFile("/Users/likhithachandra/Desktop/titles-sorted.txt")
      .rdd.zipWithIndex().map{case (l,i) =>((i+1).toInt, l)}
      .partitionBy(new HashPartitioner(1))
      .persist()
    val linkSorted = sc.read
      .textFile("/Users/likhithachandra/Desktop/simple-sorted.txt")
      .rdd.map(x => (x.split(":")(0).toInt, x.split(":")(1)))
      .partitionBy(new HashPartitioner(1))
      .persist()

    //root set
      val rootSet = indexedtitle.filter(f=> f._2.toLowerCase.contains(string.toLowerCase))
      rootSet.saveAsTextFile("/Users/likhithachandra/Desktop/PA1/rootset.txt")
      //base set
      //creating the inwards and outward link

      val outlinkForEach = linkSorted.flatMapValues(y=> y.trim.split(" +")).mapValues(x=>x.toInt)
   // flatMapValues(x => x).map{case(k,v) => (k, v.toLong)}

    //from the node
      val from_links = outlinkForEach.join(rootSet).map{case(k,(v1,v2)) => (k,v1)}

    //towards node
    val to_links = outlinkForEach.map{case(k,v) => (v,k)}.join(rootSet).map{case(k,(v1,v2)) => (v1,k)}

    //union
    var base_links = from_links.union(to_links).distinct().sortByKey()
    base_links.cache()

    var base_set = from_links.map{case(k,v) => (v,k)}.union(to_links).groupByKey().join(indexedtitle).map{case(k,(v1,v2)) => (k,v2)}
    base_set = base_set.union(rootSet).distinct().sortByKey().persist()

    base_set.cache()

    var auths = base_set.map{case(k,v) => (k, 1.0)}
    var hubs = base_set.map{case(k,v) => (k, 1.0)}


    // iterating until converge

    val max_iter = 50
    var count = 0
    while(count < max_iter){
      // println(count)

      // compute auth score
      var temp_auths = auths

      auths = base_links.join(hubs).map{case(k,(v1,v2)) => (v1,v2)}.reduceByKey((x, y) => x+y).rightOuterJoin(hubs).map{case(k,(Some(v1),v2)) => (k, v1);case(k,(None,v2)) => (k,0)}
      var auths_sum = auths.map(_._2).sum()
      auths = auths.map{case(k,v) => (k, BigDecimal(v/auths_sum).setScale(5, BigDecimal.RoundingMode.HALF_UP).toDouble)}
      auths.cache()


      var temp_hubs = hubs
      // compute hub score
      hubs = base_links.map{case(k,v) => (v,k)}.join(auths).map{case(k,(v1,v2)) => (v1,v2)}.reduceByKey((x, y) => x+y).rightOuterJoin(auths).map{case(k,(Some(v1),v2)) => (k, v1);case(k,(None,v2)) => (k,0)}
      var hubs_sum = hubs.map(_._2).sum()
      hubs = hubs.map{case(k,v) => (k, BigDecimal(v/hubs_sum).setScale(5, BigDecimal.RoundingMode.HALF_UP).toDouble)}
      hubs.cache()

      // if(auths.subtract(temp_auths).count()==0 && hubs.subtract(temp_hubs).count()==0){break}
      count = count + 1
    }

    var auths_output = auths.join(base_set).map{case(k,(v1,v2)) => (v1,v2)}.sortByKey(false)

    var hubs_output = hubs.join(base_set).map{case(k,(v1,v2)) => (v1,v2)}.sortByKey(false)

    //output

    val x =base_set.take(50);
    val y = sc.sparkContext.parallelize(x)
   // val  v= new SparkSession(sc).set("spark.some.config.option", "some-value")
    y.saveAsTextFile("/Users/likhithachandra/Desktop/PA1/baseset.txt")
    auths_output.saveAsTextFile("/Users/likhithachandra/Desktop/PA1/auth.txt")
    hubs_output.saveAsTextFile("/Users/likhithachandra/Desktop/PA1/hubs.txt")
    sc.stop()

  }



}
