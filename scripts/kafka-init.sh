#!/bin/sh
# kafka-init.sh

echo "==> Start Kafka Initialization Script..."
echo "==> Target Broker: kafka:9092"

# Вместо слепого until делаем контролируемый цикл на 10 попыток
for i in $(seq 1 10); do
  echo "==> [Attempt $i/10] Checking if Kafka is ready..."
  
  # Проверяем доступность порта обычным встроенным средством (без тяжелой утилиты)
  if kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
    echo "==> Kafka is up and responding!"
    break
  fi
  
  if [ $i -eq 10 ]; then
    echo "==> [ERROR] Kafka did not respond on kafka:9092 after 10 attempts. Exiting."
    exit 1
  fi

  echo "==> Kafka is not ready yet. Sleeping 3 seconds..."
  sleep 3
done

echo "==> Creating production topics..."

# Создаем топик явно ТРЕМЯ партициями для High-Load (чтобы оправдать наш ключ балансировки по IP!)
kafka-topics.sh --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic wb-search-queries \
  --partitions 3 \
  --replication-factor 1

echo "==> Production topics created successfully!"
exit 0