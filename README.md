# Aliz Tech Challenge 4 : FXRate-Pipeline

## Problem Statement :

There is a given input textfile (ASCII). Please create an application, that makes a new file ending with -"index.txt". To the index write every word in a new line, in alphabetical order, that occurs in the textfile. After each word please display in which line in the original textfile did the word appear. Separate the numbers with commas.

### Solution :

Developed a solution in Java to read the file and generate index file for the input.

#### Assumptions :

* Each line is terminated by new line and each word is seprated by white space.
* Index is created as case-insensitive. i.e apple & Apple are considered as one word in the output index i.e apple
* Special characters like !+.^:, are not required as part of the index file.

#### To run the Program :

##### Shell Command to execute the Pipeline :
```shell
export GOOGLE_APPLICATION_CREDENTIALS=<path to google cloud service account key>
java -jar ./target/fxrate-pipeline-bundled-1.0.0.jar \
--runner=DataflowRunner \
--project=<project-id> \
--tempLocation=<Google storage temp location> \
--stagingLocation=<Google storage staging location> \
--bigQueryDataset=<Bigquery Output Dataset Name> \
--bigQueryTable=<Bigquery Output Table Name> \
--inputFile=<input file path>
```
##### Sample Shell Command to execute the Pipeline :

```shell
export GOOGLE_APPLICATION_CREDENTIALS="/Users/jegadeshthirumeni/Downloads/credentials.json"
java -jar /Users/jegadeshthirumeni/eclipse-workspace/fxrate-pipeline/target/fxrate-pipeline-bundled-1.0.0.jar \
--runner=DataflowRunner \
--project=jeganhadoopcluster \
--tempLocation=gs://aliz-tech-challenge/temp \
--stagingLocation=gs://aliz-tech-challenge/staging \
--bigQueryDataset=aliz1 \
--bigQueryTable=fxrateAverage1 \
--inputFile=gs://solutions-public-assets/time-series-master/GBPUSD_2014_01.csv
```



