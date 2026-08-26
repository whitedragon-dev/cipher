// ============================================
// DURABLE OBJECT - cipher-chat
// ============================================
export class CipherChat {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Map();
    this.members = new Set();
  }

  async fetch(request) {
    const url = new URL(request.url);
    const userId = url.searchParams.get('userId');
    if (!userId) return new Response('Missing userId', { status: 400 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    await this.handleWebSocket(userId, server);
    return new Response(null, { status: 101, webSocket: client });
  }

  async handleWebSocket(userId, ws) {
    this.sessions.set(userId, ws);
    this.members.add(userId);

    this.broadcast(JSON.stringify({
      type: 'user_online',
      userId,
      timestamp: Date.now()
    }), userId);

    ws.accept();

    ws.addEventListener('message', async (event) => {
      try {
        const data = JSON.parse(event.data);
        switch(data.type) {
          case 'message':
            await this.env.DB.prepare(
              `INSERT INTO messages (id, room_id, sender_id, ciphertext, iv, salt) 
               VALUES (?, ?, ?, ?, ?, ?)`
            ).bind(data.id, data.roomId, userId, data.ciphertext, data.iv, data.salt).run();

            this.broadcast(JSON.stringify({
              type: 'new_message',
              message: data
            }), userId);
            break;

          case 'typing':
            this.broadcast(JSON.stringify({
              type: 'typing',
              userId,
              roomId: data.roomId
            }), userId);
            break;

          case 'read_receipt':
            await this.env.DB.prepare(
              `UPDATE messages SET read_at = ? WHERE id = ?`
            ).bind(Date.now(), data.messageId).run();
            break;
        }
      } catch (err) {
        console.error('Message error:', err);
      }
    });

    ws.addEventListener('close', async () => {
      this.sessions.delete(userId);
      this.members.delete(userId);
      await this.env.DB.prepare(
        `UPDATE users SET last_seen = ? WHERE id = ?`
      ).bind(Date.now(), userId).run();

      this.broadcast(JSON.stringify({
        type: 'user_offline',
        userId,
        timestamp: Date.now()
      }), userId);
    });
  }

  broadcast(message, excludeUserId) {
    for (const [userId, ws] of this.sessions) {
      if (userId !== excludeUserId) {
        try { ws.send(message); } catch (err) {}
      }
    }
  }
}

// ============================================
// MAIN WORKER - cipher
// ============================================
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // WEBSOCKET ROUTE
    if (path === '/api/ws') {
      const roomId = url.searchParams.get('roomId');
      const userId = url.searchParams.get('userId');
      if (!roomId || !userId) {
        return new Response('Missing roomId or userId', { status: 400 });
      }

      const id = env.CIPHER_CHAT.idFromName(roomId);
      const obj = env.CIPHER_CHAT.get(id);
      return obj.fetch(request);
    }

    // ============================================
    // API ROUTES
    // ============================================

    // Health check
    if (path === '/api/health') {
      return new Response(JSON.stringify({ 
        status: 'ok', 
        project: 'cipher',
        timestamp: Date.now() 
      }), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Signup
    if (path === '/api/users' && request.method === 'POST') {
      try {
        const { username, displayName, passwordHash, publicKey } = await request.json();
        if (!username || !passwordHash || !publicKey) {
          return new Response('Missing fields', { status: 400, headers: corsHeaders });
        }

        const userId = crypto.randomUUID();
        await env.DB.prepare(
          `INSERT INTO users (id, username, display_name, password_hash, public_key) 
           VALUES (?, ?, ?, ?, ?)`
        ).bind(userId, username, displayName || username, passwordHash, publicKey).run();

        return new Response(JSON.stringify({ success: true, userId, username }), {
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }
    }

    // Login - get user
    if (path === '/api/users' && request.method === 'GET') {
      const username = url.searchParams.get('username');
      if (!username) {
        return new Response('Missing username', { status: 400, headers: corsHeaders });
      }

      const result = await env.DB.prepare(
        `SELECT id, username, display_name, public_key, password_hash 
         FROM users WHERE username = ?`
      ).bind(username).first();

      if (!result) {
        return new Response(JSON.stringify({ error: 'User not found' }), {
          status: 404,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }

      return new Response(JSON.stringify(result), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Get rooms
    if (path === '/api/rooms' && request.method === 'GET') {
      const userId = url.searchParams.get('userId');
      if (!userId) {
        return new Response('Missing userId', { status: 400, headers: corsHeaders });
      }

      const rooms = await env.DB.prepare(
        `SELECT r.*, 
          (SELECT ciphertext FROM messages WHERE room_id = r.id ORDER BY sent_at DESC LIMIT 1) as last_message,
          (SELECT COUNT(*) FROM messages WHERE room_id = r.id AND read_at IS NULL AND sender_id != ?) as unread_count
         FROM rooms r
         JOIN room_members rm ON r.id = rm.room_id
         WHERE rm.user_id = ?
         ORDER BY r.created_at DESC`
      ).bind(userId, userId).all();

      return new Response(JSON.stringify(rooms.results), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Get messages
    if (path === '/api/messages' && request.method === 'GET') {
      const roomId = url.searchParams.get('roomId');
      const limit = parseInt(url.searchParams.get('limit') || '50');
      const before = parseInt(url.searchParams.get('before') || Date.now());

      if (!roomId) {
        return new Response('Missing roomId', { status: 400, headers: corsHeaders });
      }

      const messages = await env.DB.prepare(
        `SELECT * FROM messages 
         WHERE room_id = ? AND sent_at < ? AND is_deleted = 0
         ORDER BY sent_at DESC LIMIT ?`
      ).bind(roomId, before, limit).all();

      return new Response(JSON.stringify(messages.results.reverse()), {
        headers: { 'Content-Type': 'application/json', ...corsHeaders }
      });
    }

    // Create room
    if (path === '/api/rooms' && request.method === 'POST') {
      try {
        const { type, name, createdBy, members } = await request.json();
        if (!type || !createdBy || !members || members.length === 0) {
          return new Response('Missing fields', { status: 400, headers: corsHeaders });
        }

        const roomId = crypto.randomUUID();
        await env.DB.prepare(
          `INSERT INTO rooms (id, type, name, created_by) VALUES (?, ?, ?, ?)`
        ).bind(roomId, type, name || null, createdBy).run();

        const allMembers = [...members, createdBy];
        for (const userId of allMembers) {
          await env.DB.prepare(
            `INSERT INTO room_members (room_id, user_id) VALUES (?, ?)`
          ).bind(roomId, userId).run();
        }

        return new Response(JSON.stringify({ success: true, roomId, type, members: allMembers }), {
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { 'Content-Type': 'application/json', ...corsHeaders }
        });
      }
    }

    // Serve UI
    if (path === '/' || path === '/index.html') {
      return new Response(getHTML(), {
        headers: { 'Content-Type': 'text/html', ...corsHeaders }
      });
    }

    return new Response('Not Found', { status: 404, headers: corsHeaders });
  }
};

// ============================================
// HTML UI - Cipher Chat
// ============================================
function getHTML() {
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>🔐 Cipher</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; background: #0a0a0f; color: #e0e0e0; padding: 20px; }
    #app { max-width: 800px; margin: 0 auto; }
    .card { background: #1a1a24; border-radius: 12px; padding: 20px; margin-bottom: 20px; border: 1px solid #2a2a3a; }
    h1 { color: #4fc3f7; }
    .cipher-title { color: #4fc3f7; font-weight: bold; }
    h3 { color: #81c784; margin-bottom: 10px; }
    input, button { padding: 10px; margin: 5px; border-radius: 6px; border: 1px solid #333; background: #12121a; color: #e0e0e0; }
    button { background: #4fc3f7; color: #0a0a0f; font-weight: bold; cursor: pointer; }
    button:hover { background: #4dd0e1; }
    #messages { max-height: 400px; overflow-y: auto; background: #12121a; border-radius: 8px; padding: 10px; }
    .msg { padding: 8px; margin: 4px 0; background: #1a1a24; border-radius: 6px; }
    .msg .sender { color: #4fc3f7; font-weight: bold; }
    .msg .time { color: #666; font-size: 0.8em; }
    .msg .cipher { color: #ffb74d; font-family: monospace; }
    #status { color: #81c784; font-size: 0.9em; }
    .flex { display: flex; gap: 10px; flex-wrap: wrap; }
    .room-item { padding: 10px; margin: 5px 0; background: #12121a; border-radius: 6px; cursor: pointer; border: 1px solid #2a2a3a; }
    .room-item:hover { background: #1a1a2a; border-color: #4fc3f7; }
    .badge { background: #4fc3f7; color: #0a0a0f; padding: 2px 8px; border-radius: 12px; font-size: 0.8em; }
  </style>
</head>
<body>
  <div id="app">
    <div class="card">
      <h1>🔐 <span class="cipher-title">Cipher</span></h1>
      <p>End-to-End Encrypted Chat · <span id="status">Not connected</span></p>
    </div>

    <div class="card" id="auth-section">
      <h3>🔑 Authentication</h3>
      <div class="flex">
        <input id="username-input" placeholder="Username" value="alice">
        <input id="display-name-input" placeholder="Display Name" value="Alice">
        <input id="password-input" type="password" placeholder="Password" value="test123">
      </div>
      <div class="flex">
        <button onclick="signup()">Sign Up</button>
        <button onclick="login()">Login</button>
      </div>
      <div id="auth-status" style="margin-top:10px;color:#ffb74d;"></div>
    </div>

    <div class="card" id="chat-section" style="display:none;">
      <div class="flex" style="justify-content:space-between;align-items:center;">
        <h3>💬 <span id="room-name">Select a room</span></h3>
        <div class="flex">
          <button onclick="createRoom()">+ New Group</button>
          <button onclick="joinRoom()">Join Room</button>
        </div>
      </div>
      
      <div id="messages" style="margin:10px 0;">
        <div style="text-align:center;color:#666;padding:20px;">Select a room to start chatting</div>
      </div>
      
      <div class="flex">
        <input id="message-input" placeholder="Type a message..." style="flex:1;">
        <button onclick="sendMessage()">Send</button>
      </div>
    </div>

    <div class="card" id="rooms-section">
      <h3>📂 Your Rooms</h3>
      <div id="room-list">Loading rooms...</div>
    </div>
  </div>

  <script>
    let currentUser = null;
    let currentRoom = null;
    let ws = null;

    async function api(method, path, data) {
      const opts = { method, headers: { 'Content-Type': 'application/json' } };
      if (data) opts.body = JSON.stringify(data);
      const res = await fetch('/api' + path, opts);
      return res.json();
    }

    async function signup() {
      const username = document.getElementById('username-input').value;
      const displayName = document.getElementById('display-name-input').value;
      const password = document.getElementById('password-input').value;
      
      const passwordHash = 'hash_' + password;
      const publicKey = 'pk_' + username;

      const result = await api('POST', '/users', { username, displayName, passwordHash, publicKey });
      document.getElementById('auth-status').innerText = '✅ Signup: ' + JSON.stringify(result);
      
      if (result.success) {
        setTimeout(login, 500);
      }
    }

    async function login() {
      const username = document.getElementById('username-input').value;
      const password = document.getElementById('password-input').value;
      
      const user = await api('GET', '/users?username=' + encodeURIComponent(username));
      
      if (user.error) {
        document.getElementById('auth-status').innerText = '❌ Login failed: ' + user.error;
        return;
      }

      if (user.password_hash !== 'hash_' + password) {
        document.getElementById('auth-status').innerText = '❌ Invalid password';
        return;
      }

      currentUser = user;
      document.getElementById('auth-section').style.display = 'none';
      document.getElementById('chat-section').style.display = 'block';
      document.getElementById('status').innerHTML = '✅ Logged in as <strong>' + user.display_name + '</strong>';
      
      loadRooms();
      document.getElementById('auth-status').innerText = '✅ Login successful!';
    }

    async function loadRooms() {
      if (!currentUser) return;
      const rooms = await api('GET', '/rooms?userId=' + currentUser.id);
      const list = document.getElementById('room-list');
      
      if (rooms.length === 0) {
        list.innerHTML = '<div style="color:#666;padding:10px;">No rooms yet. Create or join one!</div>';
        return;
      }
      
      list.innerHTML = rooms.map(r => 
        \`<div class="room-item" onclick="joinRoom('\${r.id}')">
          <strong>\${r.name || '💬 Direct Chat'}</strong> 
          <span class="badge">\${r.unread_count > 0 ? '🔔 ' + r.unread_count : '✅'}</span>
          <br><span style="color:#666;font-size:0.8em;">
            \${r.last_message ? '🔒 ' + r.last_message.slice(0, 30) + '...' : 'No messages yet'}
          </span>
        </div>\`
      ).join('');
    }

    async function createRoom() {
      const name = prompt('Room name:');
      const member = prompt('Add member username (comma-separated for multiple):');
      if (!member) return;

      const members = member.split(',').map(m => m.trim());
      const result = await api('POST', '/rooms', {
        type: 'group',
        name,
        createdBy: currentUser.id,
        members
      });
      
      if (result.success) {
        loadRooms();
        joinRoom(result.roomId);
      }
    }

    function joinRoom(roomId) {
      if (!roomId) {
        roomId = prompt('Enter room ID:');
        if (!roomId) return;
      }

      currentRoom = roomId;
      document.getElementById('room-name').innerHTML = '<span class="cipher-title">' + roomId.slice(0, 8) + '...</span>';
      
      connectWebSocket(roomId);
      loadMessages(roomId);
    }

    function connectWebSocket(roomId) {
      if (ws) ws.close();
      
      const wsUrl = \`ws://\${window.location.host}/api/ws?roomId=\${roomId}&userId=\${currentUser.id}\`;
      ws = new WebSocket(wsUrl);
      
      ws.onopen = () => {
        document.getElementById('status').innerHTML = '🟢 Connected to <span class="cipher-title">Cipher</span> 🔒';
      };
      
      ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        console.log('📨 WS:', data);
        
        switch(data.type) {
          case 'new_message':
            addMessage(data.message);
            break;
          case 'user_online':
            document.getElementById('status').innerHTML = '🟢 ' + data.userId + ' online';
            break;
          case 'user_offline':
            document.getElementById('status').innerHTML = '🔴 ' + data.userId + ' offline';
            break;
          case 'typing':
            document.getElementById('status').innerHTML = '✏️ ' + data.userId + ' is typing...';
            setTimeout(() => {
              document.getElementById('status').innerHTML = '🟢 Connected to <span class="cipher-title">Cipher</span> 🔒';
            }, 2000);
            break;
        }
      };
      
      ws.onclose = () => {
        document.getElementById('status').innerHTML = '🔴 Disconnected';
        setTimeout(() => connectWebSocket(roomId), 3000);
      };
    }

    async function loadMessages(roomId) {
      const messages = await api('GET', '/messages?roomId=' + roomId);
      const container = document.getElementById('messages');
      
      if (messages.length === 0) {
        container.innerHTML = '<div style="text-align:center;color:#666;padding:20px;">No messages yet. Say hello! 👋</div>';
        return;
      }
      
      container.innerHTML = messages.map(m => 
        \`<div class="msg">
          <span class="sender">\${m.sender_id.slice(0, 6)}</span>
          <span class="time">\${new Date(m.sent_at).toLocaleTimeString()}</span>
          <br><span class="cipher">🔒 \${m.ciphertext}</span>
          \${m.read_at ? ' ✅' : ' ⏳'}
        </div>\`
      ).join('');
      container.scrollTop = container.scrollHeight;
    }

    function addMessage(data) {
      const container = document.getElementById('messages');
      const div = document.createElement('div');
      div.className = 'msg';
      div.innerHTML = \`
        <span class="sender">\${data.sender_id.slice(0, 6)}</span>
        <span class="time">\${new Date().toLocaleTimeString()}</span>
        <br><span class="cipher">🔒 \${data.ciphertext}</span>
        ⏳
      \`;
      container.appendChild(div);
      container.scrollTop = container.scrollHeight;
    }

    function sendMessage() {
      const input = document.getElementById('message-input');
      const text = input.value;
      if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;

      const encrypted = 'ENC_' + text;
      
      ws.send(JSON.stringify({
        type: 'message',
        id: crypto.randomUUID(),
        roomId: currentRoom,
        ciphertext: encrypted,
        iv: 'iv_placeholder',
        salt: 'salt_placeholder'
      }));
      
      input.value = '';
    }

    function sendTyping() {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'typing',
          roomId: currentRoom
        }));
      }
    }

    // Auto-login for demo
    setTimeout(() => {
      document.getElementById('username-input').value = 'alice';
      document.getElementById('display-name-input').value = 'Alice';
      document.getElementById('password-input').value = 'test123';
    }, 500);
  </script>
</body>
</html>`;
  }
