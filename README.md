# spark_scala_matrixinverse
Spark application for computing inverse of the matrix.

environment: Scala 2.10.4, spark 1.6.2

To run this program you need a input file with name inputmatrix.txt

sample inputmatrix.txt

2,3,1
1,2,4
5,7,9

To compile Scala code do below thing

1. download and install sbt in your system (if not available)
2. open terminal and goto folder of this project
3. $sbt (sbt prompt will open)
4. >compile (hit enter)
5. >exit (exit from sbt prompt)

To submit a job to spark you need below steps

1. sbt will make two folders project & target
2. open terminal & switch to target folder you will find the folder with your scala version
3. switch to "classes" folder under folder named with scala version (e.g. scala-2.10.4)
4. $jar -cvf matrixinverse.jar *.class
5. spark-submit --class matrixinverse_spark --master local[8] ./matrixinverse.jar 100

