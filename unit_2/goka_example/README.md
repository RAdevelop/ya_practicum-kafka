Следующий набор команд создает нужные топики:

Учитывая кластер 3c-3b из unit_1

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic input --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --config min.insync.replicas=2
```
```bash
docker exec -it kafka-b-1 kafka-topics --create --topic output --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --config min.insync.replicas=2
```
```bash
docker exec -it kafka-b-1 kafka-topics --create --topic upper-case-group-table --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2 --config min.insync.replicas=2
```