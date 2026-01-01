#!/usr/bin/env python3
"""
Script to show Kafka topic information.
"""

from kafka import KafkaConsumer, TopicPartition  # type: ignore[reportMissingImports]
from kafka.admin import KafkaAdminClient  # type: ignore[reportMissingImports]
import json

KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'kubernetes-logs'

def show_topic_info():
    """Display Kafka topic information."""
    
    print("="*70)
    print("📊 KAFKA TOPIC: kubernetes-logs")
    print("="*70)
    print(f"\n📍 Bootstrap Server: {KAFKA_BOOTSTRAP}")
    print(f"📝 Topic Name: {KAFKA_TOPIC}\n")
    
    # List topics using admin client
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            client_id='topic_info_client'
        )
        
        topics = admin_client.list_topics()
        print(f"📋 Available Topics ({len(topics)}):")
        print("-" * 70)
        for topic in sorted(topics):
            marker = "✅" if topic == KAFKA_TOPIC else "  "
            print(f"{marker} {topic}")
        print()
        admin_client.close()
        
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        print("\n💡 Make sure Kafka is running:")
        print("   - Docker: cd docker && docker-compose up -d")
        print("   - Homebrew: brew services start kafka")
        return False
    
    # Get topic details using consumer
    print("🔍 Topic Details:")
    print("-" * 70)
    
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=2000
        )
        
        # Get partitions
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        
        if partitions:
            print(f"✅ Topic '{KAFKA_TOPIC}' EXISTS")
            print(f"   Partitions: {sorted(partitions)}")
            print(f"   Total Partitions: {len(partitions)}")
            
            # Get offset information
            print(f"\n📦 Partition Information:")
            print("-" * 70)
            
            # Create TopicPartition objects
            topic_partitions = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
            consumer.assign(topic_partitions)
            
            # Get end offsets (high water marks)
            end_offsets = consumer.end_offsets(topic_partitions)
            beginning_offsets = consumer.beginning_offsets(topic_partitions)
            
            total_messages = 0
            for tp in topic_partitions:
                beg_offset = beginning_offsets.get(tp, 0)
                end_offset = end_offsets.get(tp, 0)
                msg_count = end_offset - beg_offset
                total_messages += msg_count
                
                print(f"   Partition {tp.partition}:")
                print(f"      Messages: {msg_count:,}")
                print(f"      Offset Range: {beg_offset:,} - {end_offset:,}")
            
            print(f"\n   📊 Total Messages: {total_messages:,}")
            
            # Try to read sample messages
            if total_messages > 0:
                print(f"\n📨 Sample Messages (first 3):")
                print("-" * 70)
                
                # Create new consumer for reading messages
                msg_consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=[KAFKA_BOOTSTRAP],
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    consumer_timeout_ms=3000,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
                )
                
                count = 0
                for message in msg_consumer:
                    if count >= 3:
                        break
                    print(f"\n[Message {count + 1}]")
                    print(f"   Partition: {message.partition}")
                    print(f"   Offset: {message.offset}")
                    print(f"   Key: {message.key.decode('utf-8') if message.key else 'None'}")
                    if message.value:
                        value = message.value
                        print(f"   Server ID: {value.get('server_id', 'N/A')}")
                        print(f"   Timestamp: {value.get('timestamp', 'N/A')}")
                        print(f"   Source: {value.get('source', 'N/A')}")
                        print(f"   Protocol: {value.get('protocol', 'N/A')}")
                    count += 1
                
                msg_consumer.close()
            else:
                print(f"\n⚠️  No messages in topic")
                print("   (Topic is empty - run stream_logs.py to produce messages)")
            
        else:
            print(f"⚠️  Topic '{KAFKA_TOPIC}' does not exist")
            print("   (It will be created automatically when first message is sent)")
        
        consumer.close()
        
    except Exception as e:
        print(f"❌ Error reading topic: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*70)
    print("💡 To produce messages, run:")
    print(f"   python src/data_collection/stream_logs.py")
    print("="*70)
    
    return True

if __name__ == "__main__":
    show_topic_info()
