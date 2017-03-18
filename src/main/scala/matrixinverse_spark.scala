//packages for spark core & mlib
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vectors,Vector,Matrix,SingularValueDecomposition,DenseMatrix,DenseVector}
import org.apache.spark._

//comman package for io operation
import java.io._



object matrixinverse_spark{
	//spark configuration
	val conf = new SparkConf().setAppName("matrixinverse_spark")
	val sc = new SparkContext(conf)

	/*
	 * Method for converting matrix to RDD of spark
	 * parameters: I/P -> Matrix of scala , O/P: RDD[Vector]
	*/
	def matrixToRDD(m: Matrix): RDD[Vector] = {
		val columns = m.toArray.grouped(m.numRows)
	     	val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
	     	val vectors = rows.map(row => new DenseVector(row.toArray))
	     	sc.parallelize(vectors)
	}

	/*
	 * Method for exporting spark RowMatrix to csv file
	 * parameters: I/P -> RDD[String], file name , O/P: CSV file
	*/
	def exportRowMatrix(matrix:RDD[String], fileName: String) = {
	     	val pw = new PrintWriter(fileName)
	     	matrix.collect().foreach(line => pw.println(line)) 
	     	pw.flush()
	     	pw.close()
	}
	
	/*
	 * Method for computing inverse using svd of mllib of spark
	 * parameters: I/P -> Spark RowMatrix, file name , O/P: SPark DenseMatrix
	*/
	def computeInverse(X: RowMatrix): DenseMatrix = {
	  val nCoef = X.numCols.toInt
	  val svd = X.computeSVD(nCoef, computeU = true)
	  if (svd.s.size < nCoef) {
	    sys.error("RowMatrix.computeInverse called on singular matrix.")
	  }

	  // Create the inv diagonal matrix from S 
	  val invS = DenseMatrix.diag(new DenseVector(svd.s.toArray.map(x => math.pow(x,-1))))

	  // U cannot be a RowMatrix
	  val U = new DenseMatrix(svd.U.numRows().toInt,svd.U.numCols().toInt,svd.U.rows.collect.flatMap(x => x.toArray))

	  val V = svd.V
	  // inv(X) = V*inv(S)*transpose(U)  --- the U is already transposed.
	  (V.multiply(invS)).multiply(U)
	}
	
	def main(args: Array[String]) {
		//running without input file
		
		/*val rawdata=Seq(
			Vectors.dense(2,3,1),
			Vectors.dense(1,2,4),
			Vectors.dense(5,7,9)
			)
		*/
		
		//reading a source file
		val rdd=sc.textFile("./inputmatrix.txt")
		
		//conversion of source file to rdd of vectors
		val rawdata = rdd.map(s => Vectors.dense(s.split(',').map(_.toDouble)))
		
		//conversion of rdd of vectors in to RowMatrix of spark
		val mat: RowMatrix = new RowMatrix(rawdata)
	
		val output:DenseMatrix = computeInverse(mat)
		val rows = matrixToRDD(output)
		val mat1 = new RowMatrix(rows)
		
		//conversion of row matrix to string rdd
		val rdd1 = mat1.rows.map( x => x.toArray.mkString(","))
		exportRowMatrix(rdd1,"/home/bdam/outputmatrix.csv")
	}
}
