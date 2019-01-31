export GOOGLE_APPLICATION_CREDENTIALS="/Users/jegadeshthirumeni/Downloads/credentials.json"
java -jar /Users/jegadeshthirumeni/eclipse-workspace/fxrate-pipeline/target/fxrate-pipeline-bundled-1.0.0.jar \
--runner=DataflowRunner \
--project=jeganhadoopcluster \
--tempLocation=gs://aliz-tech-challenge/temp \
--stagingLocation=gs://aliz-tech-challenge/staging \
--bigQueryDataset=aliz1 \
--bigQueryTable=fxrateAverage1 \
--inputFile=gs://solutions-public-assets/time-series-master/GBPUSD_2014_01.csv



