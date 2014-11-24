test:
	go test -v --short .

build: orkestrator.go
	gox -output "bin/{{.Dir}}_{{.OS}}_{{.Arch}}" -os "linux darwin" -arch "amd64"

startconsul:
	consul agent -server -bootstrap -data-dir /tmp/consul

stopconsul:
	killall -TERM consul


build_docker: orkestrator.go
	docker build -t jmcarbo/orkestrator .

run_docker: orkestrator.go
	docker run -ti --rm --name node1 jmcarbo/orkestrator /bin/bash

start_cluster:
	export NODE1=$(shell docker run -d -h node1 --name node1 jmcarbo/orkestrator )
	export CIP=$(shell docker inspect --format '{{ .NetworkSettings.IPAddress }}' node1)
	@echo $(CIP)
	sleep 5
	export NODE2=$(shell docker run -d -h node2 --name node2 jmcarbo/orkestrator /bin/start.sh @echo $(CIP) )
	sleep 5
	export NODE3=$(shell docker run -d -h node3 --name node3 jmcarbo/orkestrator /bin/start.sh @echo $(CIP) )


stop_cluster:
	docker rm -f node1
	docker rm -f node2
	docker rm -f node3

#CIP=$(shell docker inspect --format '{{ .NetworkSettings.IPAddress }}' node1)
run_client:
	docker -ti exec node1 /bin/bash

push:
	docker push jmcarbo/orkestrator
