import express from 'express';
import { createServer } from 'node:http';
import { Server } from 'socket.io';
import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const server = createServer(app);

// PostgreSQL connection pool
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
});

// Socket.IO configuration with environment variables for Vercel/Railway
const io = new Server(server, {
  cors: {
    origin: process.env.CLIENT_URL || 'http://localhost:5173',
    methods: ['GET', 'POST'],
    credentials: true,
  },
  transports: ['websocket', 'polling'],
  allowEIO3: true,
});

// Store active users
const users = new Map();

// ============================================
// DATABASE INITIALIZATION
// ============================================

async function initializeDatabase() {
  try {
    // Create tables if they don't exist
    await pool.query(`
      CREATE TABLE IF NOT EXISTS channels (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
        user_id VARCHAR(255) NOT NULL,
        username VARCHAR(100) NOT NULL,
        content TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS users_online (
        id VARCHAR(255) PRIMARY KEY,
        username VARCHAR(100) NOT NULL,
        status VARCHAR(50) DEFAULT 'online',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Create indexes for better performance
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_messages_channel_id ON messages(channel_id);
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);
    `);

    console.log('✅ Database initialized successfully');
  } catch (error) {
    console.error('❌ Error initializing database:', error);
    process.exit(1);
  }
}

// ============================================
// DATABASE HELPER FUNCTIONS
// ============================================

async function getOrCreateChannels() {
  try {
    const defaultChannels = [
      { id: 1, name: '#general' },
      { id: 2, name: '#random' },
      { id: 3, name: '#dev' },
      { id: 4, name: '#design' },
      { id: 5, name: '#off-topic' },
    ];

    for (const channel of defaultChannels) {
      await pool.query(
        'INSERT INTO channels (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING',
        [channel.id, channel.name]
      );
    }

    const result = await pool.query('SELECT id, name FROM channels ORDER BY id');
    return result.rows;
  } catch (error) {
    console.error('Error getting/creating channels:', error);
    return [];
  }
}

async function getChannelMessages(channelId, limit = 50) {
  try {
    const result = await pool.query(
      `SELECT id, user_id, username, content, created_at 
       FROM messages 
       WHERE channel_id = $1 
       ORDER BY created_at DESC 
       LIMIT $2`,
      [channelId, limit]
    );
    return result.rows.reverse();
  } catch (error) {
    console.error('Error fetching messages:', error);
    return [];
  }
}

async function saveMessage(channelId, userId, username, content) {
  try {
    const result = await pool.query(
      `INSERT INTO messages (channel_id, user_id, username, content) 
       VALUES ($1, $2, $3, $4) 
       RETURNING id, created_at`,
      [channelId, userId, username, content]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error saving message:', error);
    return null;
  }
}

async function addUserOnline(userId, username) {
  try {
    await pool.query(
      `INSERT INTO users_online (id, username, status) 
       VALUES ($1, $2, 'online') 
       ON CONFLICT (id) DO UPDATE SET status = 'online'`,
      [userId, username]
    );
  } catch (error) {
    console.error('Error adding user online:', error);
  }
}

async function removeUserOnline(userId) {
  try {
    await pool.query(
      'DELETE FROM users_online WHERE id = $1',
      [userId]
    );
  } catch (error) {
    console.error('Error removing user online:', error);
  }
}

async function getOnlineUsers() {
  try {
    const result = await pool.query(
      'SELECT id, username, status FROM users_online ORDER BY created_at DESC'
    );
    return result.rows;
  } catch (error) {
    console.error('Error fetching online users:', error);
    return [];
  }
}

// ============================================
// EXPRESS ROUTES
// ============================================

app.get('/', (req, res) => {
  res.json({
    message: 'Chatify Server',
    version: '1.0.0',
    status: 'running',
    environment: process.env.NODE_ENV || 'development',
  });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ============================================
// SOCKET.IO EVENT HANDLERS
// ============================================

io.on('connection', async (socket) => {
  console.log(`✅ Usuario conectado: ${socket.id}`);

  // Send channels list on connection
  const channels = await getOrCreateChannels();
  socket.emit('channels-list', channels);

  // Send online users
  const onlineUsers = await getOnlineUsers();
  io.emit('users-list', onlineUsers);

  // ============================================
  // JOIN CHANNEL
  // ============================================
  socket.on('join-channel', async (channelId, username) => {
    try {
      const finalUsername = username || `Usuario-${socket.id.slice(0, 5)}`;
      
      // Store user data
      users.set(socket.id, {
        id: socket.id,
        username: finalUsername,
        channelId: channelId,
        status: 'online',
      });

      // Add user to database
      await addUserOnline(socket.id, finalUsername);

      // Join socket room
      socket.join(`channel-${channelId}`);

      // Load previous messages
      const messages = await getChannelMessages(channelId);
      socket.emit('load-messages', messages);

      // Notify others in the channel
      socket.to(`channel-${channelId}`).emit('user-joined', {
        username: finalUsername,
        totalUsers: users.size,
      });

      // Update users list globally
      const updatedUsers = await getOnlineUsers();
      io.emit('users-list', updatedUsers);

      console.log(`👤 ${finalUsername} joined channel ${channelId}`);
    } catch (error) {
      console.error('Error joining channel:', error);
      socket.emit('error', { message: 'Error joining channel' });
    }
  });

  // ============================================
  // RECEIVE MESSAGE (chat message event)
  // ============================================
  socket.on('chat message', async (msg) => {
    try {
      console.log('💬 message: ' + msg);
      const user = users.get(socket.id);
      
      if (!user) {
        console.warn('⚠️ Usuario no encontrado para socket:', socket.id);
        return;
      }

      const channelId = user.channelId || 1; // Default to general channel

      // Insert into database and get ID (offset)
      let result;
      try {
        result = await pool.query(
          'INSERT INTO messages (channel_id, user_id, username, content) VALUES ($1, $2, $3, $4) RETURNING id',
          [channelId, user.id, user.username, msg]
        );

        // Include the offset (ID) with the message
        io.emit('chat message', msg, result.rows[0].id);
        console.log('✅ Mensaje guardado con ID:', result.rows[0].id);
      } catch (e) {
        console.error('❌ Error inserting message:', e);
        socket.emit('error', { message: 'Error inserting message' });
        return;
      }
    } catch (error) {
      console.error('❌ Error in chat message handler:', error);
      socket.emit('error', { message: 'Error sending message' });
    }
  });

  // ============================================
  // RECEIVE MESSAGE (chat-message event - alternative format)
  // ============================================
  socket.on('chat-message', async (data) => {
    try {
      const user = users.get(socket.id);
      
      if (!user || !data.channelId || !data.message) {
        return;
      }

      // Save message to database
      const savedMessage = await saveMessage(
        data.channelId,
        user.id,
        user.username,
        data.message
      );

      if (savedMessage) {
        const messageData = {
          id: savedMessage.id,
          user_id: user.id,
          username: user.username,
          content: data.message,
          created_at: savedMessage.created_at,
          timestamp: new Date(savedMessage.created_at).toLocaleTimeString('es-MX', {
            hour: '2-digit',
            minute: '2-digit',
          }),
        };

        // Emit to all users in the channel
        io.to(`channel-${data.channelId}`).emit('receive-message', messageData);
        
        // Also emit as 'chat message' for compatibility
        io.emit('chat message', data.message, savedMessage.id);
      }
    } catch (error) {
      console.error('Error saving message:', error);
      socket.emit('error', { message: 'Error sending message' });
    }
  });

  // ============================================
  // UPDATE USER STATUS
  // ============================================
  socket.on('user-status', async (status) => {
    try {
      const user = users.get(socket.id);
      if (user) {
        user.status = status;
        
        // Update in database
        await pool.query(
          'UPDATE users_online SET status = $1 WHERE id = $2',
          [status, socket.id]
        );

        // Broadcast updated users list
        const updatedUsers = await getOnlineUsers();
        io.emit('users-list', updatedUsers);
      }
    } catch (error) {
      console.error('Error updating user status:', error);
    }
  });

  // ============================================
  // DISCONNECT
  // ============================================
  socket.on('disconnect', async () => {
    try {
      const user = users.get(socket.id);
      
      if (user) {
        // Remove from database
        await removeUserOnline(socket.id);
        
        // Remove from memory
        users.delete(socket.id);

        // Notify users in the channel
        if (user.channelId) {
          io.to(`channel-${user.channelId}`).emit('user-left', {
            username: user.username,
            totalUsers: users.size,
          });
        }

        // Update global users list
        const updatedUsers = await getOnlineUsers();
        io.emit('users-list', updatedUsers);

        console.log(`❌ Usuario desconectado: ${user.username} (${socket.id})`);
      }
    } catch (error) {
      console.error('Error on disconnect:', error);
    }
  });

  // ============================================
  // ERROR HANDLING
  // ============================================
  socket.on('error', (error) => {
    console.error('Socket error:', error);
  });
});

// ============================================
// GRACEFUL SHUTDOWN
// ============================================

process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing HTTP server');
  server.close(async () => {
    await pool.end();
    process.exit(0);
  });
});

process.on('SIGINT', async () => {
  console.log('SIGINT signal received: closing HTTP server');
  server.close(async () => {
    await pool.end();
    process.exit(0);
  });
});

// ============================================
// START SERVER
// ============================================

const PORT = process.env.PORT || 3000;

async function startServer() {
  try {
    // Test database connection
    await pool.query('SELECT NOW()');
    console.log('✅ Database connection successful');

    // Initialize database tables
    await initializeDatabase();

    // Start server - Listen on all interfaces for Railway/Vercel
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`🚀 Server running on port ${PORT}`);
      console.log(`📊 Environment: ${process.env.NODE_ENV || 'development'}`);
      console.log(`🌐 CORS origin: ${process.env.CLIENT_URL || 'http://localhost:5173'}`);
      console.log(`✅ Ready to accept connections`);
    });
  } catch (error) {
    console.error('❌ Error starting server:', error);
    process.exit(1);
  }
}

startServer();

// Export for testing if needed
export { app, io, pool };