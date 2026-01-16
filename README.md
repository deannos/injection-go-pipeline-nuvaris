### Project Basic Structure 

```text
injection-go-pipeline-nuvaris/
├── cmd/
│   └── injector/
│       └── main.go         # Application entry point
├── internal/
│   ├── config/             # Configuration handling
│   │   └── config.go
│   ├── injector/           # Core injection service logic
│   │   └── service.go
│   ├── model/              # Data models
│   │   └── electricity_bill.go
│   └── server/             # HTTP server setup
│       └── server.go
├── pkg/
│   └── kafka/              # Kafka producer wrapper (optional, could be in internal/injector)
│       └── producer.go
├── configs/
│   └── config.yaml         # Configuration file (e.g., for Viper)
├── docker-compose.yml     # For Kafka (and potentially the app)
├── .gitignore
├── go.mod
├── go.sum
└── README.md

```

### Back of Envelope Calculations 

- Target : ( 1M - 3M )events/min

`HTTP → IngestBill → eventChan → worker → Kafka AsyncProducer → broker
`

```text
Max TPS = min(
  HTTP accept rate,
  eventChan drain rate,
  worker processing rate,
  Kafka producer rate
)

```
Conservative real-world number (Go + Sarama):
`~15–30 µs per bill per worker
`
Let’s take the worse number:
- 30 µs = 0.00003 sec

So per worker 
- 1 / 0.00003 ≈ 33,333 bills/sec/worker

Worker pool capacity:

- Let W - number of workers
- Worker Capacity = W * 33,333 bills/sec

```text
| Workers | Capacity (bills/sec) | Capacity (bills/min) |
| ------- | -------------------- | -------------------- |
| 4       | 133k                 | ~8M                  |
| 8       | 266k                 | ~16M                 |
| 16      | 533k                 | ~32M                 |
| 32      | 1.06M                | ~64M                 |
```

Kafka producer throughput :

- Single Kafka producer, async, compression enabled.
- On localhost Kafka (even dev):
  - 100k–300k msgs/sec  ----> conservative scenario 
  - 500k+ msgs/sec  ---> typical scenario 
  - With Batching -----> 1M+ msgs/sec

##### Lets cap Kafka at :
```text
Kafka capacity ≈ 300,000 bills/sec
```

##### HTTP + channel admission calculation

Let:
C = eventChan capacity
D = drain rate (bills/sec)
I observed:
- bursts of 10,000 bills instantly
- then 503s

```text
that means  burst > C
```
```bash 
if 
C = 10,000
D = 30,000 bills/sec

then buffer drains in: 
10,000 / 30,000 = 0.33 sec

So any burst faster than 0.33s will overflow → 503.


```

##### Note : If i conclude the sustained throughput equation

- Incoming rate ≤ drain rate
  // Drain rate ≈ Kafka producer rate (because async).

So, Max sustainable throughput ≈ Kafka throughput ≈ 300k bills/sec ≈ 18M bills/min


#### Final Throughput Calculation 

```text
Max bills/sec ≈ min(
  Kafka async throughput,
  W / per-bill-cost,
  HTTP admission rate
)

```


### kafka commands 

#### Note : Kafka CLI tools are located under /opt/kafka/bin in the official Apache Kafka Docker image. Commands must be run from there or referenced with full path.

1. you will be inside the Kafka Container 
```
docker exec -it kafka-injector-kafka bash
cd /opt/kafka/bin

```

2. Broker : 
```
localhost:9092
```

3. Check broker is reachable 
```
kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

4. Check running Kafka process
```
ps aux | grep kafka
```

5. List all Topics 
```text
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

```

6. Describe a topic ( leader, ISR and partitions )
```text
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic electricity-bills

```
#### Note : Look for
- Leader != -1
- Isr not empty
- partition count

7. Create a topic 
```text
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic electricity-bills-dlq \
  --partitions 3 \
  --replication-factor 1

```
8. Increase partitions( only forward )
```text
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic electricity-bills \
  --partitions 6
```

#### Note : cannot decrease partitions

9. Delete topic (if enabled)
```text
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic electricity-bills-dlq
```

10. Consume Messages 

- consume from beginning 
```text
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic electricity-bills \
  --from-beginning

```

- consume only new messages 
```text
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic electricity-bills

```

- consume with partition + offset 
```text
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic electricity-bills \
  --partition 0 \
  --offset 0

```
- Pretty print JSON 
```text
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic electricity-bills \
  --from-beginning | jq

```

11. Produce Messages 

- Simple Producer
```text
./kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic electricity-bills

```

#### Note : type JSON & press Enter

- Produce with Key ( partitioning test )
```text
./kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic electricity-bills \
  --property "parse.key=true" \
  --property "key.separator=:"

```
#### Example 
```bash
customer-1:{"bill_id":"123"}


same key ---> same partition
```

12. Consumer Groups & Lag

- List Consumer Groups 
```text
./kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

- Describe group ( Lag check )
```text
./kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group console-consumer \
  --describe

```

#### Note : Key columns
- CURRENT-OFFSET
- LOG_END_OFFSET
- LAG


13. DLQ checks 

- Consume DLQ
```text
./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic electricity-bills-dlq \
  --from-beginning

```

- Check if DLQ is empty 
```text
./kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic electricity-bills-dlq
```

14. Kafka Logs

- Kafka logs directory 
```text
ls /opt/kafka/logs

```

- Tail server Logs 
```text
tail -f /opt/kafka/logs/server.log
```

- Controller log (KRaft mode)
```text
tail -f /opt/kafka/logs/controller.log

```

15. Config Inspection 

- Describe topic configs 
```text
./kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name electricity-bills \
  --describe

```

### Note : If Kafka “isn’t working”, I check in this order:
- 1️⃣ `kafka-broker-api-versions.sh` → broker alive
- 2️⃣ `kafka-topics.sh --describe` → leader & ISR
- 3️⃣ `kafka-console-producer.sh` → can I write?
- 4️⃣ `kafka-console-consumer.sh` → can I read?
- 5️⃣ `kafka-consumer-groups.sh` → lag growing?
- 6️⃣ Kafka logs → broker errors