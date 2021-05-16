# ProcessingBigData

Tips:
1. csv files are included in the folders. They are the raw data needed to be processed.
2. You may need to upload all necessary data and source code to HDFS first before attempting to operate on the data.
3. The code only operates on cluster systems. You can use Hadoop to execute the jar file with a correct file path. 

Code Functionalities:
1. ETL
1.1 In this folder, there are four csv files of clean League of Legends game data, each was put in one of the sub-folders. The cleaning code focuses on choosing the relevant columns and fills null values with 0 to avoid null exceptions. The four versions of code are generally the same thing besides some small differences due to various level of sparsity in the data.
2. Profiling
2.1 In this folder, there are three java files, each contains a part of the profiling source code. Unlike the cleaning code, there are no jar files so the source code needs to be manually compiled from java file to class file, then to jar file. After that, you could use the resulted jar file to process the raw data. 

#jave to class
javac -classpath `yarn classpath` -d . MaxTemperatureMapper.java
javac -classpath `yarn classpath` -d . MaxTemperatureReducer.java
javac -classpath `yarn classpath`:. -d . MaxTemperature.java
Note: It's important to use the correct backtick in the commands above.

#class to jar
jar -cvf maxTemp.jar *.class

#hdfs create new directory and populate directory wtih file
hdfs dfs -mkdir hw hdfs dfs -mkdir hw/input
hdfs dfs -put input.txt project/profile

#hadoop execution
hadoop jar jar_file_path job_name input_file_in_hdfs output_path
(e.g. hadoop jar maxTemp.jar MaxTemperature input.txt /user/<netID>/<myDir>/output)

#copy file to cluster
Use your computer's own console and type:
scp source\path server_address:destination
(e.g. scp C:\Users\S1999\Desktop\a\*   sh5005@peel.hpc.nyu.edu:/home/sh5005/project/profile)

