import json
import subprocess
from confluent_kafka import Consumer


# ==========================================
# Kafka Configuration
# ==========================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "cisco.commands"
KAFKA_GROUP_ID = "cisco-workers"


# ==========================================
# Cisco Router Configuration
# ==========================================

CISCO_HOST = "192.168.79.134"
CISCO_PORT = 22

CISCO_USERNAME = "admin"
CISCO_PASSWORD = "cisco"


# ==========================================
# Kafka Consumer
# ==========================================

consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])


# ==========================================
# Execute Cisco Command
# ==========================================

def execute_command(command):

    print()
    print("----------------------------------------")
    print("Executing Cisco command:")
    print(command)
    print("----------------------------------------")

    ssh_command = [
        "ssh",
        "-tt",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "KexAlgorithms=+diffie-hellman-group14-sha1",
        "-o", "HostKeyAlgorithms=+ssh-rsa",
        "-o", "PubkeyAuthentication=no",
        "-p", str(CISCO_PORT),
        f"{CISCO_USERNAME}@{CISCO_HOST}",
        command
    ]

    result = subprocess.run(
        ssh_command,
        input=CISCO_PASSWORD + "\n",
        text=True,
        capture_output=True
    )

    output = result.stdout
    error = result.stderr

    if error:
        print("SSH message:")
        print(error)

    return output


# ==========================================
# Main
# ==========================================

def main():

    print()
    print("----------------------------------------")
    print("Cisco Kafka Worker")
    print("----------------------------------------")
    print(f"Kafka server : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic  : {KAFKA_TOPIC}")
    print(f"Kafka group  : {KAFKA_GROUP_ID}")
    print(f"Cisco host   : {CISCO_HOST}")
    print("----------------------------------------")

    try:

        print()
        print("Testing Cisco SSH connection...")

        test_command = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "KexAlgorithms=+diffie-hellman-group14-sha1",
            "-o", "HostKeyAlgorithms=+ssh-rsa",
            "-o", "PubkeyAuthentication=no",
            "-p", str(CISCO_PORT),
            f"{CISCO_USERNAME}@{CISCO_HOST}",
            "show ip interface brief"
        ]

        test_result = subprocess.run(
            test_command,
            input=CISCO_PASSWORD + "\n",
            text=True,
            capture_output=True
        )

        if test_result.returncode != 0:

            print("Cisco SSH connection failed.")
            print(test_result.stderr)

            return

        print("Connected to Cisco router!")
        print()
        print("Cisco test output:")
        print("----------------------------------------")
        print(test_result.stdout)
        print("----------------------------------------")

        print()
        print("Waiting for Kafka commands...")
        print("----------------------------------------")

        while True:

            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():

                print("Kafka error:")
                print(msg.error())

                continue

            try:

                message = msg.value().decode("utf-8")

                print()
                print("Received Kafka message:")
                print(message)

                data = json.loads(message)

                command = data.get("command")

                if not command:

                    print("ERROR: No command found")
                    continue

                print()
                print("Command received:")
                print(command)

                output = execute_command(command)

                print()
                print("Cisco output:")
                print("----------------------------------------")
                print(output)
                print("----------------------------------------")

                consumer.commit(msg)

                print("Message processed successfully.")

            except json.JSONDecodeError:

                print("ERROR: Invalid JSON message")

            except Exception as e:

                print()
                print("ERROR processing message:")
                print(e)

    except KeyboardInterrupt:

        print()
        print("Stopping worker...")

    except Exception as e:

        print()
        print("Worker error:")
        print(e)

    finally:

        consumer.close()

        print("Kafka consumer closed.")
        print("Worker stopped.")


# ==========================================
# Start Worker
# ==========================================

if __name__ == "__main__":
    main()
