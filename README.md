# cassandra-debug
Debug utility for Cassandra

# Tunneling into a Cassandra host to debug it

If the Cassandra host does not have its port visible, then tunnelling through SSH can be done using:

```
ssh -Nf -L 9042:127.0.0.1:9042 cassandra-b4.ala.org.au
```

After that, connecting to localhost/127.0.0.1 will transparently connect to the remote Cassandra instance.
