FROM java:8
ADD ./target/scala-2.11/SolrSupervisor-assembly-1.0.jar watcher.jar
CMD ["java", "-jar", "watcher.jar"]
