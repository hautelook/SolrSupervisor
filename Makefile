target/scala-2.11/SolrSupervisor-assembly-1.0.jar: sbt-launch.jar
	java -jar sbt-launch.jar assembly

IMAGE:=docker.hautelook.net/solr-supervisor
VERSION:=$(shell git rev-parse --short=10 HEAD)

build:
	docker build -t $(IMAGE):$(VERSION) .
	docker tag -f $(IMAGE):$(VERSION) $(IMAGE):latest

upload:
	docker push $(IMAGE):$(VERSION)
	docker push $(IMAGE):latest
