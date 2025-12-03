/**
 * React Hooks Example
 * Demonstrates using useSubscription hook in a React application
 */

import React, { useState, useMemo } from 'react';
import { VibeSqlClient } from '../../src/client';
import { useSubscription, useQuery } from '../../src/react/index';

interface Message {
  id: number;
  channel_id: string;
  user_id: number;
  text: string;
  created_at: Date;
}

interface User {
  id: number;
  username: string;
}

interface ChatRoomProps {
  channelId: string;
  db: VibeSqlClient;
  currentUserId: number;
}

/**
 * Component that displays messages in a channel
 */
export function ChatRoom({
  channelId,
  db,
  currentUserId,
}: ChatRoomProps) {
  const [newMessage, setNewMessage] = useState('');
  const [sending, setSending] = useState(false);

  // Subscribe to messages
  const {
    data: messages,
    error: messagesError,
    isLoading: messagesLoading,
  } = useSubscription<Message>(
    db,
    'SELECT * FROM messages WHERE channel_id = $1 ORDER BY created_at DESC LIMIT 100',
    [channelId]
  );

  // Load users
  const {
    data: users,
    error: usersError,
    isLoading: usersLoading,
  } = useQuery<User>(db, 'SELECT * FROM users');

  // Create username map
  const userMap = useMemo(() => {
    const map = new Map<number, string>();
    if (users) {
      for (const user of users) {
        map.set(user.id, user.username);
      }
    }
    return map;
  }, [users]);

  // Handle sending a message
  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!newMessage.trim()) {
      return;
    }

    try {
      setSending(true);

      await db.query(
        'INSERT INTO messages (channel_id, user_id, text) VALUES ($1, $2, $3)',
        [channelId, currentUserId, newMessage]
      );

      setNewMessage('');
    } catch (error) {
      console.error('Failed to send message:', error);
      alert(`Error sending message: ${
        error instanceof Error ? error.message : 'Unknown error'
      }`);
    } finally {
      setSending(false);
    }
  };

  // Error state
  if (messagesError) {
    return (
      <div className="error">
        <h2>Error loading messages</h2>
        <p>{messagesError.message}</p>
      </div>
    );
  }

  // Loading state
  if (messagesLoading) {
    return (
      <div className="loading">
        <p>Loading messages...</p>
      </div>
    );
  }

  return (
    <div className="chat-room">
      <div className="chat-header">
        <h2>#{channelId}</h2>
        {usersLoading && <span className="loading-indicator">⟳</span>}
      </div>

      <div className="messages-list">
        {messages && messages.length > 0 ? (
          // Reverse to show newest at bottom
          [...messages]
            .reverse()
            .map(msg => (
              <div
                key={msg.id}
                className={`message ${
                  msg.user_id === currentUserId ? 'own-message' : ''
                }`}
              >
                <div className="message-header">
                  <strong className="username">
                    {userMap.get(msg.user_id) || `User ${msg.user_id}`}
                  </strong>
                  <span className="timestamp">
                    {msg.created_at instanceof Date
                      ? msg.created_at.toLocaleTimeString()
                      : msg.created_at}
                  </span>
                </div>
                <div className="message-text">{msg.text}</div>
              </div>
            ))
        ) : (
          <div className="empty-state">
            <p>No messages yet. Start the conversation!</p>
          </div>
        )}
      </div>

      <form className="message-form" onSubmit={handleSendMessage}>
        <input
          type="text"
          value={newMessage}
          onChange={e => setNewMessage(e.target.value)}
          placeholder="Type a message..."
          disabled={sending}
          className="message-input"
          maxLength={500}
        />
        <button
          type="submit"
          disabled={!newMessage.trim() || sending}
          className="send-button"
        >
          {sending ? 'Sending...' : 'Send'}
        </button>
      </form>
    </div>
  );
}

/**
 * Example app with multiple channels
 */
export function ChatApp({ db }: { db: VibeSqlClient }) {
  const [channelId, setChannelId] = useState('general');
  const currentUserId = parseInt(
    process.env.REACT_APP_USER_ID || '1'
  );

  return (
    <div className="chat-app">
      <div className="sidebar">
        <div className="channel-list">
          <h3>Channels</h3>
          {['general', 'random', 'dev', 'design'].map(channel => (
            <button
              key={channel}
              className={`channel-button ${
                channelId === channel ? 'active' : ''
              }`}
              onClick={() => setChannelId(channel)}
            >
              #{channel}
            </button>
          ))}
        </div>
      </div>

      <div className="main-content">
        <ChatRoom
          channelId={channelId}
          db={db}
          currentUserId={currentUserId}
        />
      </div>
    </div>
  );
}

// Styles (can be moved to CSS file)
const styles = `
.chat-app {
  display: flex;
  height: 100vh;
  background: #f5f5f5;
}

.sidebar {
  width: 250px;
  background: #2c2f33;
  color: white;
  padding: 20px;
  overflow-y: auto;
}

.channel-list h3 {
  margin: 0 0 15px 0;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 1px;
}

.channel-button {
  display: block;
  width: 100%;
  padding: 10px;
  margin: 5px 0;
  background: transparent;
  border: none;
  color: #72767d;
  cursor: pointer;
  text-align: left;
  font-size: 14px;
  border-radius: 4px;
  transition: all 0.2s;
}

.channel-button:hover {
  background: #3c3f45;
  color: white;
}

.channel-button.active {
  background: #7289da;
  color: white;
}

.main-content {
  flex: 1;
  display: flex;
  flex-direction: column;
  background: white;
}

.chat-room {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.chat-header {
  padding: 15px 20px;
  border-bottom: 1px solid #eee;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.messages-list {
  flex: 1;
  overflow-y: auto;
  padding: 20px;
}

.message {
  margin-bottom: 15px;
  padding: 10px;
  background: #f9f9f9;
  border-radius: 4px;
}

.message.own-message {
  background: #dfe6ff;
  margin-left: 50px;
}

.message-header {
  display: flex;
  justify-content: space-between;
  margin-bottom: 5px;
  font-size: 12px;
}

.username {
  color: #7289da;
}

.timestamp {
  color: #999;
}

.message-text {
  color: #2c2f33;
  word-wrap: break-word;
}

.empty-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: #999;
}

.message-form {
  display: flex;
  padding: 15px 20px;
  border-top: 1px solid #eee;
  gap: 10px;
}

.message-input {
  flex: 1;
  padding: 10px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 14px;
  font-family: inherit;
}

.send-button {
  padding: 10px 20px;
  background: #7289da;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-weight: bold;
}

.send-button:hover:not(:disabled) {
  background: #5e70ba;
}

.send-button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.error {
  padding: 20px;
  background: #f8d7da;
  color: #721c24;
  border-radius: 4px;
}

.loading {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
}

.loading-indicator {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
`;
