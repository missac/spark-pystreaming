spark streaming frame use python

source support: kafka  socket
ouptput support: elasticsearch postgres

usage:
  first config the conf.ini with source and output
  then write your code in process.py to deal your data
  then run:
 ./bin/spark-submit --master spark://192.168.3.6:21001 --conf spark.task.id=hhhhh  --jars ./jars/spark-streaming-kafka-0-8-assembly_2.11-2.0.2.jar,./jars/postgresql-42.0.0.jar ./mustangstream.py

