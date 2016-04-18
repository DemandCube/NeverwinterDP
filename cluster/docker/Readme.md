To ssh to the default machine:

````
$docker-machine -D ssh default
````

get the IP from your default docker machine and ssh to it:

`````
$docker-machine ip default
#this prints something like this out: 192.168.99.100

$ssh docker@192.168.99.100
#password is tcuser but you can also use the identity file, see other answer
`````
To ssh to docker container via private network, you need to route the private ip through docker-machine host

````
$sudo route -n add 172.17.0.0/16 $(docker-machine ip default)
````



