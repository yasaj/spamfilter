// spamTopWord.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object spamTopWord {

  def probaWordDir(sc:SparkContext)(filesDir:String)
  :(RDD[(String, Double)], Long) = {

     val textFiles = sc.wholeTextFiles(filesDir)
     val nbFiles = textFiles.count()
     val stopwords = List(".",":",","," ", "-","/","\\", "\'","(",")","@",":")
     val words = textFiles.map({case (key, values) => (key, values.split(" ").distinct.filter(!stopwords.contains(_)))})
     val pairswords = words.flatMap({case (s,values)=>(values.map(s=>(s,1)))})
     val wordDirOccurence = pairswords.reduceByKey(_+_)
     val probaWord = wordDirOccurence.map({case (s,l)=>(s, l.toDouble/nbFiles)})
      
      
    
    (probaWord, nbFiles)

   
  }

  def log2(x: Double) : Double = scala.math.log(x) / scala.math.log(2) // Compute the log2 of a double.
  def log2RDD(proba: RDD[(String, Double)]) : RDD[(String, Double)] = {
      val temp = proba.map{case (key, values) => (key, log2(values))}
      temp
  }

  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double):RDD[(String, Double)] = {
      
      
    //val MIF = probaWC.union(log2RDD(probaWC.union(probaW).reduceByKey((a,b)=>a/(b*probaC))).reduceByKey((a,b)=>a*b))
                               
   // MIF
      
      val JoinW_WC = probaW.leftOuterJoin(probaWC)
                val JoinW_WC_ac_default= JoinW_WC.map(x => (x._1, (x._2._1, x._2._2.getOrElse(probaDefault))))
                val MIF = JoinW_WC_ac_default.map(x => (x._1, x._2._2 * (math.log(x._2._2 / (x._2._1 * probaC) ) / (math.log(2.0)))))
                MIF
  }


 
  def main(sc:SparkContext)(args: Array[String]) = {
      
      
      val (probaWordHam,nbFilesHam) = probaWordDir(sc:SparkContext)("/tmp/ling-spam/ham")
      val (probaWordSpam,nbFilesSpam) = probaWordDir(sc:SparkContext)("/tmp/ling-spam/spam")
      
      val probaWordHamTrue = probaWordHam
      val probaWordHamFalse = probaWordHam.map{case (key, values) => (key, 1.0-values)}
      val probaWordSpamTrue = probaWordSpam
      val probaWordSpamFalse = probaWordSpam.map{case (key, values) => (key, 1.0-values)}
      
     
      val probaW = probaWordDir(sc:SparkContext)("/tmp/ling-spam/*/*.txt")._1
      val probaWF = probaW.map{case (key, values) => (key, 1.0-values)}
      
      val probaCHam = 2412.0 /(481+2412)
      val probaCSpam = 481.0 /(481+2412)
      
      val probaDefault = 0.2/(2412+481)
      
      
      val MIF1 = computeMutualInformationFactor(probaWordHamTrue, probaW, probaCHam, probaDefault)
      val MIF2 = computeMutualInformationFactor(probaWordHamFalse, probaWF, probaCHam, probaDefault)
      val MIF3 = computeMutualInformationFactor(probaWordSpamTrue, probaW, probaCSpam, probaDefault)
      val MIF4 = computeMutualInformationFactor(probaWordSpamFalse, probaWF, probaCSpam, probaDefault)
      
    
      
      
      val MI = MIF1.union(MIF2).reduceByKey(_+_)
      MI.takeOrdered(20)(Ordering[Double].reverse.on(x=>x._2))
      
      
      
      
      
      
      
      
      
      
    
  }

} // end of spamTopWord





