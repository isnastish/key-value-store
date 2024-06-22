## Overview
KVS is a remote key-value storage written in Golang. Currently it supports storing POD types, like integers, 32-bit floats, strings and hashmaps of type `map[string]string`, but I am planning to extend the functionality in the future and use it in my chat application as a replacement or alternative for a redis backend. The application is based around REST api and all the operations on a storage are done through the basic HTTP calls, which should be replaced with remote procedural calls using gRPC (it has a lot of room for improvements, which I left for myself, or for a reader, as an exercise). The architecutre document will be added as soon as I find the time for it.

## Running KVS service in a docker container
In order to run a KVS service in a docker container and execute tests against it you should have docker installed on your machine. If you do, build a docker image with the following command `docker build --tag kvs:1.0.0 .`. A dot at the end is important, it's a hint to a docker where to search for a `.Dockerfile`, which is in the root directory of our project. After building the image, run `docker run --rm -ti --name=kvs-service kvs:1.0.0`. If you have done everything correctly, you should see the logs that the service is listening on a port `:8080`. 
```log
{"level":"info","timestamp":"Sat Jun 22 18:17:15 UTC 2024","message":"Listening :8080"}
```
Now, go to visual studio and execute any client's test. You can do it via the terminal with `go test ./... -count=1` command  as well. You should see the logs comming from the kvs service running in the docker container.
```log
{"level":"info","timestamp":"Sat Jun 22 16:52:48 UTC 2024","message":"Endpoint /api/v1-0-0/strget/dummy_String, method GET"}
{"level":"info","timestamp":"Sat Jun 22 16:52:48 UTC 2024","message":"Endpoint /api/v1-0-0/mapput/randomMap, method PUT"}
{"level":"info","timestamp":"Sat Jun 22 16:52:48 UTC 2024","message":"Endpoint /api/v1-0-0/mapget/randomMap, method GET"}
{"level":"info","timestamp":"Sat Jun 22 16:52:48 UTC 2024","message":"Endpoint /api/v1-0-0/mapdel/randomMap, method DELETE"}
...
...
```
