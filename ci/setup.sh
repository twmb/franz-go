/kafka/bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config 'SCRAM-SHA-256=[password=admin-secret-256],SCRAM-SHA-512=[password=admin-secret-512]' --entity-type users --entity-name adminscram

