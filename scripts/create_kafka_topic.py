#!/usr/bin/env python3
"""
Script to create Kafka topic with specified number of partitions.
"""

from kafka.admin import KafkaAdminClient, NewTopic  # type: ignore[reportMissingImports]
from kafka.errors import TopicAlreadyExistsError  # type: ignore[reportMissingImports]
import sys

KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'kubernetes-logs'
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1

def create_topic(num_partitions=3, delete_existing=False):
    """
    Create Kafka topic with specified number of partitions.
    
    Args:
        num_partitions: Number of partitions (default: 3)
        delete_existing: If True, delete existing topic first (default: False)
    """
    
    print("="*70)
    print("🔧 KAFKA TOPIC CREATION")
    print("="*70)
    print(f"\n📍 Bootstrap Server: {KAFKA_BOOTSTRAP}")
    print(f"📝 Topic Name: {KAFKA_TOPIC}")
    print(f"📊 Partitions: {num_partitions}")
    print(f"🔄 Replication Factor: {REPLICATION_FACTOR}\n")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            client_id='topic_creator'
        )
        
        # Check if topic exists
        existing_topics = admin_client.list_topics()
        topic_exists = KAFKA_TOPIC in existing_topics
        
        if topic_exists:
            print(f"⚠️  Topic '{KAFKA_TOPIC}' already exists")
            
            if delete_existing:
                print(f"🗑️  Deleting existing topic...")
                from kafka.admin import ConfigResource, ConfigResourceType
                from confluent_kafka.admin import AdminClient as ConfluentAdminClient
                
                # Try using kafka-python's delete_topics (if available)
                try:
                    # Note: kafka-python doesn't have delete_topics in older versions
                    # We'll need to use kafka CLI or confluent-kafka
                    print("❌ Cannot delete topic using kafka-python directly")
                    print("💡 Please delete the topic manually using Kafka CLI:")
                    print(f"   kafka-topics --delete --topic {KAFKA_TOPIC} --bootstrap-server {KAFKA_BOOTSTRAP}")
                    print("\n   Or use Docker exec:")
                    print(f"   docker exec -it kafka kafka-topics --delete --topic {KAFKA_TOPIC} --bootstrap-server localhost:9092")
                    admin_client.close()
                    return False
                except Exception as e:
                    print(f"❌ Error: {e}")
                    admin_client.close()
                    return False
            else:
                print(f"💡 To recreate with {num_partitions} partitions, you need to:")
                print(f"   1. Delete the existing topic (data will be lost)")
                print(f"   2. Run this script again")
                print(f"\n   Or increase partitions (keeps data):")
                print(f"   docker exec -it kafka kafka-topics --alter --topic {KAFKA_TOPIC} --partitions {num_partitions} --bootstrap-server localhost:9092")
                admin_client.close()
                return False
        
        # Create new topic
        print(f"📦 Creating topic '{KAFKA_TOPIC}' with {num_partitions} partitions...")
        
        topic = NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=num_partitions,
            replication_factor=REPLICATION_FACTOR
        )
        
        admin_client.create_topics([topic])
        print(f"✅ Topic '{KAFKA_TOPIC}' created successfully!")
        print(f"   Partitions: {num_partitions}")
        print(f"   Replication Factor: {REPLICATION_FACTOR}")
        
        admin_client.close()
        return True
        
    except TopicAlreadyExistsError:
        print(f"⚠️  Topic '{KAFKA_TOPIC}' already exists")
        print("💡 Use --delete flag to recreate it")
        return False
    except Exception as e:
        print(f"❌ Error creating topic: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        print("\n" + "="*70)

def increase_partitions(num_partitions=3):
    """
    Increase partitions of existing topic (keeps existing data).
    Note: This requires using Kafka CLI as kafka-python doesn't support it directly.
    """
    print("="*70)
    print("📈 INCREASE KAFKA TOPIC PARTITIONS")
    print("="*70)
    print(f"\n📍 Bootstrap Server: {KAFKA_BOOTSTRAP}")
    print(f"📝 Topic Name: {KAFKA_TOPIC}")
    print(f"📊 Target Partitions: {num_partitions}\n")
    
    print("💡 To increase partitions (keeps existing data), run:")
    print(f"   docker exec -it kafka kafka-topics --alter \\")
    print(f"     --topic {KAFKA_TOPIC} \\")
    print(f"     --partitions {num_partitions} \\")
    print(f"     --bootstrap-server localhost:9092")
    print("\n   Or if using Kafka directly:")
    print(f"   kafka-topics --alter --topic {KAFKA_TOPIC} --partitions {num_partitions} --bootstrap-server {KAFKA_BOOTSTRAP}")
    print("\n" + "="*70)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Create or modify Kafka topic')
    parser.add_argument('--partitions', type=int, default=NUM_PARTITIONS, 
                       help=f'Number of partitions (default: {NUM_PARTITIONS})')
    parser.add_argument('--delete', action='store_true', 
                       help='Delete existing topic before creating (WARNING: loses data)')
    parser.add_argument('--increase', action='store_true',
                       help='Show command to increase partitions (keeps data)')
    
    args = parser.parse_args()
    
    if args.increase:
        increase_partitions(args.partitions)
    else:
        create_topic(num_partitions=args.partitions, delete_existing=args.delete)

