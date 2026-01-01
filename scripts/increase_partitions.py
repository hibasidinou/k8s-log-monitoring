#!/usr/bin/env python3
"""
Script to increase Kafka topic partitions using Kafka Admin API.
This works without Docker - just needs Kafka to be accessible.
"""

from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType  # type: ignore[reportMissingImports]
from kafka import KafkaConsumer  # type: ignore[reportMissingImports]
import sys

KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'kubernetes-logs'
TARGET_PARTITIONS = 3

def increase_partitions():
    """
    Increase partitions of existing Kafka topic.
    Note: Kafka Admin API in kafka-python doesn't directly support altering partitions.
    We'll use the confluent-kafka library if available, or provide instructions.
    """
    
    print("="*70)
    print("📈 INCREASE KAFKA TOPIC PARTITIONS")
    print("="*70)
    print(f"\n📍 Bootstrap Server: {KAFKA_BOOTSTRAP}")
    print(f"📝 Topic Name: {KAFKA_TOPIC}")
    print(f"📊 Target Partitions: {TARGET_PARTITIONS}\n")
    
    try:
        # Check current partition count
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            consumer_timeout_ms=2000
        )
        
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        consumer.close()
        
        if not partitions:
            print(f"❌ Topic '{KAFKA_TOPIC}' does not exist")
            return False
        
        current_partitions = len(partitions)
        print(f"📊 Current Partitions: {current_partitions}")
        
        if current_partitions >= TARGET_PARTITIONS:
            print(f"✅ Topic already has {current_partitions} partition(s) (target: {TARGET_PARTITIONS})")
            return True
        
        print(f"\n⚠️  kafka-python doesn't support altering partitions directly")
        print(f"💡 You have two options:\n")
        
        print(f"Option 1: Use Kafka CLI (if installed locally)")
        print(f"   kafka-topics --alter \\")
        print(f"     --topic {KAFKA_TOPIC} \\")
        print(f"     --partitions {TARGET_PARTITIONS} \\")
        print(f"     --bootstrap-server {KAFKA_BOOTSTRAP}\n")
        
        print(f"Option 2: Use confluent-kafka Python library")
        print(f"   pip install confluent-kafka")
        print(f"   Then run this script with confluent-kafka support\n")
        
        # Try using confluent-kafka if available
        try:
            from confluent_kafka.admin import AdminClient, NewPartitions  # type: ignore[reportMissingImports]
            
            print("🔄 Attempting to use confluent-kafka...")
            
            admin_client = AdminClient({
                'bootstrap.servers': KAFKA_BOOTSTRAP
            })
            
            # Create NewPartitions object
            new_partitions = NewPartitions(TARGET_PARTITIONS)
            
            # Alter partitions
            futures = admin_client.create_partitions({
                KAFKA_TOPIC: new_partitions
            })
            
            # Wait for result
            for topic, future in futures.items():
                try:
                    future.result()  # Wait for the operation to complete
                    print(f"✅ Successfully increased partitions to {TARGET_PARTITIONS}")
                    
                    # Verify
                    consumer = KafkaConsumer(
                        bootstrap_servers=[KAFKA_BOOTSTRAP],
                        consumer_timeout_ms=2000
                    )
                    partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
                    consumer.close()
                    
                    if partitions and len(partitions) == TARGET_PARTITIONS:
                        print(f"✅ Verified: Topic now has {TARGET_PARTITIONS} partition(s)")
                        return True
                    else:
                        print(f"⚠️  Verification failed: Topic has {len(partitions) if partitions else 0} partition(s)")
                        return False
                        
                except Exception as e:
                    print(f"❌ Error increasing partitions: {e}")
                    return False
            
        except ImportError:
            print("❌ confluent-kafka not installed")
            print("   Install it with: pip install confluent-kafka")
            return False
        except Exception as e:
            print(f"❌ Error using confluent-kafka: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        print("\n" + "="*70)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Increase Kafka topic partitions')
    parser.add_argument('--partitions', type=int, default=TARGET_PARTITIONS,
                       help=f'Target number of partitions (default: {TARGET_PARTITIONS})')
    
    args = parser.parse_args()
    TARGET_PARTITIONS = args.partitions
    
    success = increase_partitions()
    sys.exit(0 if success else 1)

