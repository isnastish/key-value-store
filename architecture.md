---
title: "Architecture"
weight: 2
---

## KVS service
KVS is a key-value storage service, aka Redis. The server part is built on top of the ["gorilla/mux](https://github.com/gorilla/mux) package. It exposes multiple endpoints for each storage type with an ability to put/get and delete keys from the storage. Endpoints are constructed in the following way: `/[storage type]-[method]/{key:[0-9A-Za-z_]+}>` where the `storage-type` corresponds to the type of storage. 
For example for a map storage it would be `map` and a `method` describes what operation needs to be performed. When we want to put an element into the storage, the method would be `put`, thus the whole URI would be the following `/map-put/{key:[0-9A-Za-z_]+}`, which is pretty straightforward. In addition, 
the server exposes some other endpoints for testing the connectivity and deleting keys, hence, `/hello` endpoint could be used to retrieve an information about KVS service from the client side, `/echo` just returns a string with all the upper-case letters replace with lower-case and vise versa, `/fibo` endpoint does exactly what it says, it computes the fibonacci sequence number provided the index, the index is embedded into a URI as a query parameter `/fibo?query=n`, where `n` is the actual index. These are the testing endpoints and don't modify the state of the storage. All the operations that modify the storage use `PUT` http method, to get a value from the storage we use `GET` method and to delete a key from the storage we use a `DELETE` http method respectively. All special endpoints, like fibo, hello and echo use `POST` http method. A `/del/{key:[0-9a-zA-Z_]+}` endpoint is used to delete a key from any storage. If a provided key does exist, it sets `Deleted` http header to true which interpreted by any client, either Golang or a Python, as deleted, otherwise it is considered that the key didn't exist. 

[comment]: <> (Describe what happens when an error encountered when we hit storage's endpoint, and which status code is returned on success.)

[comment]: <> (Specify that each storage maintains a set of handlers, put/get and delete handlers.)

### Storage
Currently, the storage is capable of handling 5 data types, 32-bit signed/unsigned integers, floats, strings, and hash maps of type `map[string]string` in a Golang world. Each storage is represented as an interface of the following form 
```go
type Storage interface {
	Put(key string, cmd *CmdResult) *CmdResult
	Get(key string, cmd *CmdResult) *CmdResult
	Del(key string, CmdResult *CmdResult) *CmdResult
}
```
`Put` method is used to insert elements into the storage, `Get` to delete elements from the storage and `Del` to delete keys from the storage respectively. Every storage is a map from a string toan  underlying type that it holds, and protected with `sync.RWMutex`. 


## KVS Client


## Transaction service (TXN)

### PostgreSQL database transaction handling

### File transaction handling