#!/usr/bin/env python3
"""
Test script to send a message to Kafka and verify it's received.
"""

from kafka import KafkaProducer, KafkaConsumer, TopicPartition  # type: ignore[reportMissingImports]
from kafka.errors import KafkaError  # type: ignore[reportMissingImports]
import json
import time

KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'kubernetes-logs'

def test_kafka_send():
    """Test sending a message to Kafka."""
    
    print("="*70)
    print("🧪 TEST: Envoi de message à Kafka")
    print("="*70)
    
    # Test message
    test_message = {
        'timestamp': '2025-01-01T12:00:00',
        'server_id': 'test-server-01',
        'source': 'TEST',
        'src_ip': '192.168.1.1',
        'src_port': 8080,
        'dst_ip': '192.168.1.2',
        'dst_port': 443,
        'protocol': 'TCP',
        'duration': 0.001,
        'packets': 10,
        'bytes': 1024,
        'cpu': 0.5,
        'memory_mb': 100,
        'network_rx_kb': 50,
        'log_text': '[TEST] Test message'
    }
    
    # Create producer
    print(f"\n📡 Création du producer...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True
        )
        print("✅ Producer créé")
    except Exception as e:
        print(f"❌ Erreur création producer: {e}")
        return False
    
    # Send message
    print(f"\n📨 Envoi du message de test...")
    try:
        future = producer.send(
            KAFKA_TOPIC,
            key='test-server-01',
            value=test_message
        )
        
        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        print(f"✅ Message envoyé avec succès!")
        print(f"   Topic: {record_metadata.topic}")
        print(f"   Partition: {record_metadata.partition}")
        print(f"   Offset: {record_metadata.offset}")
        
    except Exception as e:
        print(f"❌ Erreur envoi message: {e}")
        import traceback
        traceback.print_exc()
        producer.close()
        return False
    
    # Flush to ensure message is sent
    print(f"\n⏳ Flush des messages...")
    try:
        producer.flush(timeout=10)
        print("✅ Flush réussi")
    except Exception as e:
        print(f"⚠️  Erreur flush: {e}")
    
    producer.close()
    
    # Wait a bit for Kafka to process
    print(f"\n⏳ Attente 2 secondes pour que Kafka traite le message...")
    time.sleep(2)
    
    # Verify message was received
    print(f"\n🔍 Vérification du message dans le topic...")
    try:
        # First, get partition info without subscribing
        info_consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=2000
        )
        
        # Get partitions
        partitions = info_consumer.partitions_for_topic(KAFKA_TOPIC)
        if partitions:
            print(f"   Partitions disponibles: {sorted(partitions)}")
            
            # Get offsets using TopicPartition
            topic_partitions = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
            info_consumer.assign(topic_partitions)
            
            end_offsets = info_consumer.end_offsets(topic_partitions)
            beginning_offsets = info_consumer.beginning_offsets(topic_partitions)
            
            total_messages = 0
            for tp in topic_partitions:
                beg_offset = beginning_offsets.get(tp, 0)
                end_offset = end_offsets.get(tp, 0)
                msg_count = end_offset - beg_offset
                total_messages += msg_count
                print(f"   Partition {tp.partition}: {msg_count:,} messages (offset {beg_offset:,} - {end_offset:,})")
            
            print(f"\n   📊 Total Messages: {total_messages:,}")
            
            info_consumer.close()
            
            # Now read messages with a separate consumer
            if total_messages > 0:
                print(f"\n✅ SUCCESS! Les messages sont bien dans Kafka!")
                
                # Try to read the last message
                print(f"\n📖 Lecture du dernier message...")
                read_consumer = KafkaConsumer(
                    bootstrap_servers=[KAFKA_BOOTSTRAP],
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    consumer_timeout_ms=3000,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
                )
                
                read_partitions = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
                read_consumer.assign(read_partitions)
                
                count = 0
                last_msg = None
                for message in read_consumer:
                    last_msg = message
                    count += 1
                    if count >= total_messages:
                        break
                
                if last_msg:
                    print(f"   Dernier message lu:")
                    print(f"   - Partition: {last_msg.partition}")
                    print(f"   - Offset: {last_msg.offset}")
                    print(f"   - Key: {last_msg.key.decode('utf-8') if last_msg.key else 'None'}")
                    if last_msg.value:
                        print(f"   - Server ID: {last_msg.value.get('server_id', 'N/A')}")
                        print(f"   - Source: {last_msg.value.get('source', 'N/A')}")
                
                read_consumer.close()
            else:
                print(f"\n⚠️  Aucun message trouvé dans le topic")
        else:
            print(f"⚠️  Topic non trouvé")
        
        info_consumer.close()
        
    except Exception as e:
        print(f"❌ Erreur vérification: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*70)
    return True

if __name__ == "__main__":
    test_kafka_send()

