        # kafka — Метрики, lag monitoring, consumer health

        Homework-шаблон для урока **l5_observability** (Метрики, lag monitoring, consumer health) на платформе Vibe Learn.

        ## Что делать

        Реализуй Prometheus exporter на Go, который:
- подключается к Kafka через AdminClient (confluent-kafka-go или segmentio/kafka-go);
- для заданной consumer group считает lag по каждой партиции (latest_offset − committed_offset);
- экспонирует метрику `kafka_consumer_lag{topic, partition, group}` на `/metrics` endpoint;
- обновляет метрику каждые 10 секунд.

В репозитории есть `docker-compose.yml` с Kafka + Prometheus + Grafana:
- Prometheus scrapes exporter каждые 15 секунд;
- Grafana dashboard показывает lag по партициям в реальном времени.

CI тест (Go test + testcontainers или docker-compose):
1. Поднять Kafka + exporter.
2. Producer пишет 100 сообщений в топик `test-lag-topic`.
3. Assert: метрика `kafka_consumer_lag` > 0 (lag появился).
4. Consumer дренирует топик.
5. Assert: метрика `kafka_consumer_lag` == 0 (lag исчез).

## Контекст (из transfer-задачи урока)

Тебя просят построить on-call playbook для production Kafka-кластера, который обслуживает
три системы: payment-processor (consumer group, обрабатывает транзакции), analytics-pipeline
(consumer group, считает агрегаты), и order-api (producer, пишет заказы).

**Задание:**

## Recap из урока

- Consumer lag = latest_offset − committed_offset per partition. Это главный SLI для любого Kafka consumer: он прямо отражает отставание бизнес-логики от входящего потока данных.
- UnderReplicatedPartitions > 0 — критический alert: durability деградирована, следующий сбой брокера может привести к потере данных. Реагировать немедленно.
- AdminClient API и kafka-consumer-groups.sh позволяют получить lag программно без запуска дополнительного consumer — это основа для Prometheus-экспортеров и Burrow.
- buffer-available-bytes → 0 означает back-pressure на producer: брокер не успевает принимать данные. Нужно увеличить batch.size / linger.ms или масштабировать кластер.
- Метрики — для real-time alerting; логи — для root-cause analysis после alert. Логировать каждое сообщение в poll-loop — антипаттерн: overkill по IO и CPU, не даёт оперативной картины.

        ## Как работать

        1. Платформа Vibe Learn создаёт копию этого репо в твоём GitHub-аккаунте по клику «Начать домашку» на странице урока (через GitHub `/generate`, codecrafters-pattern).
        2. Склонируй копию локально, реализуй TODO в `main.go`, прогони тесты, запушь.
        3. CI (`.github/workflows/ci.yml`) запускает `go vet` + `go test ./...` на каждый push. Платформа слушает результат через webhook от GitHub Actions и обновляет статус домашки на странице урока.

        ## Локальное окружение

        - Go 1.22+
        - Docker + docker-compose — `docker compose -f docker-compose.yml up -d` поднимает 3-нодовый Kafka cluster на портах 9092/9093/9094, использовать в тестах через bootstrap `localhost:9092,localhost:9093,localhost:9094`.

        ## Запуск

        ```bash
        # Поднять локальный Kafka
        docker compose up -d

        # Прогнать тесты (часть из них стартует свой ephemeral testcontainers cluster, часть использует docker-compose выше)
        go test ./...

        # Запустить main (печатает marker; замени stub на реализацию)
        go run .
        ```

        ## Заметка автора

        Это baseline-шаблон, сгенерированный платформой. Бизнес-сущность задачи (что конкретно реализовать в `main.go`, какие тесты сделать строгими) расширяется по ходу итераций — параллельно с углублением теории урока.
