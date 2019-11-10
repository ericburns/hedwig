# Hedwig 🦉
Hedwig Broker System. [Read specification here](https://github.com/moechaieb/hedwig).


### How to

First, boot a Redis instance locally:

```
redis-server
```

Tail logs:
```
tail -f log/hedwig.log
```

Visualize queue sizes:
```
bin/visualize-queues
```

Run a broker (run one per partition for scheduling completeness):
```
bin/boot-broker -p <PARTITION_ID=1,2,3>
```

Simulate load:
```
bin/simulate-load
```

Run a worker (runs on all partitions):
```
bin/boot-worker
```
