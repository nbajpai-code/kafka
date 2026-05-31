const { Kafka, PartitionAssigners } = require('kafkajs');

const brokerAddress = 'localhost:9092';
const topicName = 'orders-node';
const groupId = 'node-billing-group';

// Initialize core Kafka client
const kafka = new Kafka({
  clientId: 'node-client',
  brokers: [brokerAddress],
});

// Capture CLI arguments
const mode = process.argv.includes('--mode=consumer') ? 'consumer' : 'producer';

async function runProducer() {
  console.log(`🚀 Starting Node.js Producer. Target topic: ${topicName}`);
  const producer = kafka.producer({
    idempotent: true, // Safeguard against network duplicate writes
    maxInFlightRequests: 1,
  });

  await producer.connect();
  console.log('✅ Producer socket connected to cluster.');

  let count = 0;
  const interval = setInterval(async () => {
    if (count >= 10) {
      clearInterval(interval);
      console.log('Producer finished sending batch of 10 messages. Disconnecting...');
      await producer.disconnect();
      console.log('Shutdown complete.');
      process.exit(0);
    }

    const order = {
      order_id: `ORD-NODE-${Math.floor(Math.random() * 100000)}`,
      customer_id: `CUST-NODE-${Math.floor(Math.random() * 500) + 100}`,
      amount: parseFloat((Math.random() * 200 + 10).toFixed(2)),
      timestamp: new Date().toISOString(),
    };

    try {
      await producer.send({
        topic: topicName,
        acks: -1, // acks=all
        messages: [
          {
            key: order.customer_id,
            value: JSON.stringify(order),
          },
        ],
      });
      console.log(`✅ Order sent: ${order.order_id} | Customer: ${order.customer_id} | Amount: $${order.amount}`);
      count++;
    } catch (err) {
      console.error(`❌ Write failed: ${err.message}`);
    }
  }, 1000);

  // Handle graceful stop
  const gracefulShutdown = async () => {
    console.log('\nStopping producer gracefully...');
    clearInterval(interval);
    await producer.disconnect();
    console.log('Shutdown complete.');
    process.exit(0);
  };
  process.on('SIGINT', gracefulShutdown);
  process.on('SIGTERM', gracefulShutdown);
}

async function runConsumer() {
  console.log(`📥 Starting Node.js Consumer. Group: ${groupId}`);
  const consumer = kafka.consumer({
    groupId: groupId,
  });

  await consumer.connect();
  console.log('✅ Consumer socket connected to cluster.');
  
  await consumer.subscribe({ topic: topicName, fromBeginning: true });

  // Run consumer loop
  await consumer.run({
    autoCommit: false, // Turn off auto-commits to support At-Least-Once processing
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key ? message.key.toString() : 'No Key';
      const valueRaw = message.value.toString();

      try {
        const order = JSON.parse(valueRaw);
        console.log(`📦 [Process Success] Order: ${order.order_id} | Key: ${key} | Partition: ${partition} | Offset: ${message.offset} | Amt: $${order.amount}`);
        
        // Commit offset manually after processing completed successfully
        await consumer.commitOffsets([
          { topic, partition, offset: (BigInt(message.offset) + 1n).toString() }
        ]);
      } catch (err) {
        console.warn(`⚠️ Toxic message skipped: ${err.message}`);
        // Commit bad message's offset to avoid partition ingestion locks
        await consumer.commitOffsets([
          { topic, partition, offset: (BigInt(message.offset) + 1n).toString() }
        ]);
      }
    },
  });

  // Handle graceful stop
  const gracefulShutdown = async () => {
    console.log('\n🛑 Signal received. Stopping consumer gracefully...');
    await consumer.disconnect();
    console.log('Shutdown complete.');
    process.exit(0);
  };
  process.on('SIGINT', gracefulShutdown);
  process.on('SIGTERM', gracefulShutdown);
}

// Run selected mode
if (mode === 'producer') {
  runProducer().catch(console.error);
} else {
  runConsumer().catch(console.error);
}
