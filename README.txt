==============================================================================
BIG DATA ANALYTICS PRACTICALS  (from Big_Data_Practicals_Guide_Windows.docx)
==============================================================================

Put this whole folder at  C:\bigdata  - the steps in every STEPS.txt use
paths like C:\bigdata\prac1, and practical 5's code uses C:\bigdata\prac5.

START HERE
----------
  1. Read 00_SETUP_STEPS.txt and install everything once
     (Python 3.11, VS Code, Java 17, Maven, sbt, Python libraries, winutils,
      Docker Desktop).
  2. Open a practical folder in VS Code (File > Open Folder).
  3. Follow that folder's STEPS.txt. All code files are already created,
     so you can skip the "create the file / paste the code" steps.

FOLDERS
-------
  No.  Folder  Topic                         Tool                        Language  Docker
  ---  ------  ----------------------------  --------------------------  --------  ------
  1    prac1   Data skew and shuffle         Spark                       Python    No
  2    prac2   ACID and time travel          Delta Lake on Spark         Python    No
  3    prac3   3-broker cluster              Kafka                       Python    Yes
  4    prac4   Schema evolution              Kafka Schema Registry       Python    Yes
  5    prac5   Live word count               Spark Structured Streaming  Python    No
  6    prac6   Fraud detection with state    Flink                       Java      No
  7    prac7   Watermarks and late data      Flink (PyFlink)             Python    No
  8    prac8   PageRank                      Spark GraphX                Scala     No
  9    prac9   Query two sources together    Trino                       SQL       Yes
  10   prac10  Real-time dashboard           Kafka, ClickHouse, Grafana  Python    Yes

FILES IN EACH FOLDER
--------------------
  prac1   skew.py
  prac2   delta_demo.py
  prac3   docker-compose.yml, producer.py, consumer.py
  prac4   docker-compose.yml, schema_demo.py
  prac5   socket_sender.py, word_count_stream.py
  prac6   pom.xml, src\main\java\FraudDetectionJob.java
  prac7   latedata.py
  prac8   build.sbt, data\vertices.csv, data\edges.csv,
          src\main\scala\GraphXPageRank.scala
  prac9   docker-compose.yml, mysql\init.sql, etc\catalog\mysql.properties,
          etc\catalog\memory.properties, trino_queries.sql (SQL to type in Trino)
  prac10  docker-compose.yml, Dockerfile, requirements.txt, producer.py,
          consumer.py, grafana_query.sql (query to paste in Grafana)
  + every folder has STEPS.txt: aim, simple explanation, numbered steps,
    "if something goes wrong", the code with explanations, expected output.

NOTES
-----
  * prac8: the guide names this folder GraphX_PageRank_Practical; here it is
    prac8. The name does not matter - run "sbt run" inside the folder.
  * Docker practicals (3, 4, 9, 10) use some of the same ports. Run
    "docker compose down" in one before starting the next.
