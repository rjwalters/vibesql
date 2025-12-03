/**
 * Basic Query Example
 * Demonstrates simple query execution
 */

import { VibeSqlClient } from '../../src/client';

interface User {
  id: number;
  name: string;
  email: string;
  created_at: Date;
}

async function main() {
  // Create client
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

    // Execute a simple query
    console.log('\nExecuting query: SELECT * FROM users');
    const users = await db.query<User>('SELECT * FROM users LIMIT 10');

    console.log(`\nFound ${users.length} users:`);
    for (const user of users) {
      console.log(`  - ${user.name} (${user.email})`);
    }

    // Execute a parameterized query
    console.log('\nExecuting parameterized query');
    const userId = 1;
    const result = await db.query<User>(
      'SELECT * FROM users WHERE id = $1',
      [userId]
    );

    if (result.length > 0) {
      const user = result[0];
      console.log(`\nUser #${userId}:`);
      console.log(`  Name: ${user.name}`);
      console.log(`  Email: ${user.email}`);
      console.log(`  Created: ${user.created_at}`);
    }

    // Count query
    console.log('\nCounting users...');
    const countResult = await db.query<{ count: number }>(
      'SELECT COUNT(*) as count FROM users'
    );
    console.log(`Total users: ${countResult[0].count}`);
  } catch (error) {
    console.error('Error:', error instanceof Error ? error.message : error);
  } finally {
    // Close connection
    console.log('\nClosing connection...');
    await db.close();
  }
}

main().catch(console.error);
