import express from 'express';
import { createServer } from 'node:http';
import { Server } from 'socket.io';

const app = express();
const server = createServer(app);

const io = new Server(server, {
  cors: {
    origin: 'http://localhost:5173',
    methods: ['GET', 'POST'],
  },
});

// Almacenar usuarios conectados
const users = new Map();
const channels = new Map();

// Inicializar canales
const defaultChannels = [
  { id: 1, name: '#general' },
  { id: 2, name: '#random' },
  { id: 3, name: '#dev' },
  { id: 4, name: '#design' },
  { id: 5, name: '#off-topic' },
];

defaultChannels.forEach((channel) => {
  channels.set(channel.id, {
    ...channel,
    messages: [],
  });
});

app.get('/', (req, res) => {
  res.send('<h1>Chatify Server</h1>');
});

io.on('connection', (socket) => {
  console.log(`✅ Usuario conectado: ${socket.id}`);

  // Enviar lista de canales al cliente
  socket.emit('channels-list', defaultChannels);

  // Enviar lista de usuarios conectados
  socket.emit('users-list', Array.from(users.values()));

  // Cuando un usuario se une a un canal
  socket.on('join-channel', (channelId, username) => {
    const user = {
      id: socket.id,
      username: username || `Usuario-${socket.id.slice(0, 5)}`,
      status: 'online',
    };

    users.set(socket.id, user);
    socket.join(`channel-${channelId}`);

    const channel = channels.get(channelId);
    if (channel) {
      // Enviar mensajes previos del canal
      socket.emit('load-messages', channel.messages);

      // Notificar a todos en el canal que alguien se unió
      io.to(`channel-${channelId}`).emit('user-joined', {
        username: user.username,
        totalUsers: users.size,
      });
    }

    // Actualizar lista de usuarios para todos
    io.emit('users-list', Array.from(users.values()));
  });

  // Cuando se envía un mensaje
  socket.on('chat-message', (data) => {
    const user = users.get(socket.id);
    if (user && data.channelId) {
      const message = {
        id: Date.now(),
        user: user.username,
        message: data.message,
        timestamp: new Date().toLocaleTimeString([], {
          hour: '2-digit',
          minute: '2-digit',
        }),
        userId: socket.id,
      };

      // Guardar en el canal
      const channel = channels.get(data.channelId);
      if (channel) {
        channel.messages.push(message);

        // Enviar a todos en el canal
        io.to(`channel-${data.channelId}`).emit('receive-message', message);
      }
    }
  });

  // Actualizar estado del usuario
  socket.on('user-status', (status) => {
    const user = users.get(socket.id);
    if (user) {
      user.status = status;
      io.emit('users-list', Array.from(users.values()));
    }
  });

  // Cuando un usuario se desconecta
  socket.on('disconnect', () => {
    const user = users.get(socket.id);
    users.delete(socket.id);

    console.log(`❌ Usuario desconectado: ${socket.id}`);

    if (user) {
      io.emit('user-left', {
        username: user.username,
        totalUsers: users.size,
      });
    }

    // Actualizar lista de usuarios
    io.emit('users-list', Array.from(users.values()));
  });
});

server.listen(3000, () => {
  console.log('🚀 Servidor ejecutándose en http://localhost:3000');
});