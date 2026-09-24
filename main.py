import json
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "cisco.commands"


app = FastAPI(
    title="Cisco Command API",
    description="Backend API for sending Cisco commands to Kafka",
    version="1.0.0",
)


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }
)


class CommandRequest(BaseModel):
    device_id: str
    command: str


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Kafka message delivered: "
            f"topic={msg.topic()}, "
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )


def create_topic():
    admin_client = AdminClient(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
        }
    )

    metadata = admin_client.list_topics(timeout=5)

    if KAFKA_TOPIC in metadata.topics:
        print(f"Kafka topic already exists: {KAFKA_TOPIC}")
        return

    topic = NewTopic(
        KAFKA_TOPIC,
        num_partitions=3,
        replication_factor=1,
    )

    futures = admin_client.create_topics([topic])

    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"Kafka topic created: {topic_name}")
        except Exception as e:
            print(f"Failed to create topic {topic_name}: {e}")


@app.on_event("startup")
def startup_event():
    try:
        create_topic()
    except Exception as e:
        print(f"Kafka startup error: {e}")


@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "Cisco Command API",
        "kafka": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC,
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "kafka": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC,
    }


@app.post("/commands")
def send_command(request: CommandRequest):
    message_id = str(uuid.uuid4())

    message = {
        "message_id": message_id,
        "device_id": request.device_id,
        "command": request.command,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        producer.produce(
            KAFKA_TOPIC,
            key=request.device_id,
            value=json.dumps(message),
            callback=delivery_report,
        )

        producer.flush(timeout=10)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send message to Kafka: {str(e)}",
        )

    return {
        "status": "sent",
        "message_id": message_id,
        "topic": KAFKA_TOPIC,
        "message": message,
    }
