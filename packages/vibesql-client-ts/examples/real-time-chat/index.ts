/**
 * Real-Time Chat Example
 * Demonstrates subscription-based real-time updates
 */

import { VibeSqlClient } from '../../src/client';

interface Message {
  id: number;
  channel_id: string;
  user_id: number;
  text: string;
  created_at: Date;
}

async function main() {
  const db = new VibeSqlClient({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'mydb',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD,
  });

  try {
    // Connect to database
    console.log('Connecting to database...');
    await db.connect();
    console.log('Connected!');

    const channelId = process.env.CHANNEL_ID || 'general';

    // Subscribe to messages in a channel
    console.log(`\nSubscribing to messages in #${channelId}...`);

    const subscription = db.subscribe<Message>(
      'SELECT * FROM messages WHERE channel_id = $1 ORDER BY created_at DESC LIMIT 50',
      [channelId],
      {
        // Called with initial results and on every update
        onData: messages => {
          console.log(`\n📨 Received ${messages.length} messages:`);
          for (const msg of messages.slice(-5)) {
            // Show last 5
            const date = msg.created_at instanceof Date
              ? msg.created_at.toLocaleTimeString()
              : msg.created_at;
            console.log(`  [${date}] User ${msg.user_id}: ${msg.text}`);
          }
        },

        // Called for incremental updates
        onDelta: delta => {
          if (delta.type === 'insert') {
            const msg = delta.row;
            const date = msg.created_at instanceof Date
              ? msg.created_at.toLocaleTimeString()
              : msg.created_at;
            console.log(
              `\n✨ New message: [${date}] User ${msg.user_id}: ${msg.text}`
            );
          } else if (delta.type === 'update') {
            const newMsg = delta.newRow;
            console.log(`\n✏️  Edited: User ${newMsg.user_id}: ${newMsg.text}`);
          } else if (delta.type === 'delete') {
            console.log(`\n🗑️  Message deleted`);
          }
        },

        // Called on error
        onError: error => {
          console.error('❌ Subscription error:', error.message);
        },
      }
    );

    // Keep subscription active for a bit
    console.log(
      '\nListening for messages... (press Ctrl+C to stop)'
    );

    // In a real application, you would keep the subscription active
    // For this example, we'll wait for a while
    await new Promise(resolve => setTimeout(resolve, 60000));

    subscription.unsubscribe();
  } catch (error) {
    console.error('Error:', error instanceof Error ? error.message : error);
  } finally {
    console.log('\nClosing connection...');
    await db.close();
  }
}

main().catch(console.error);
