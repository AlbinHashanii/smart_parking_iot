from confluent_kafka.admin import AdminClient, NewTopic

# Kafka configuration
conf = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(conf)

# Create parking_data topic
def create_topic():
    topic_list = [
        NewTopic('parking_data', num_partitions=3, replication_factor=1)
    ]
    fs = admin_client.create_topics(topic_list)
    for topic, f in fs.items():
        try:
            f.result()  # Wait for operation to complete
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

if __name__ == '__main__':
    create_topic()