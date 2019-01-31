java -jar fxrate-pipeline-1.0.0.jar 
--runner=DataflowRunner \
--project=jeganhadoopcluster \
--serviceAccountKey=/Users/jegadeshthirumeni/Downloads/My Project-33b07955afa7.json \
--stagingLocation=gs://aliz-tech-challenge/staging \
--tempLocation=gs://aliz-tech-challenge/temp \
--bigQueryDataset=aliz1 \
--bigQueryTable=fxrate_average \
--input=gs://solutions-public-assets/time-series-master/GBPUSD_2014_01.csv