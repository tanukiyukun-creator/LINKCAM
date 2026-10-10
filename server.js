'use strict';
const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');
const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e6, cors: { origin: false } });
app.use(express.static(path.join(__dirname, 'public'), { etag: true, maxAge: '1h' }));
app.get('/health', (_req, res) => res.json({ app: 'LINKCAM', status: 'ok' }));
const rooms = new Map();
const safe = (v, n=80) => typeof v === 'string' ? v.trim().slice(0,n) : '';
function sendRoom(room, except, payload) {
  const r = rooms.get(room); if (!r) return;
  for (const [id, p] of r.peers) if (id !== except) p.socket.emit('signal', payload);
}
io.on('connection', socket => {
  let membership = null;
  socket.on('join', (data, ack=()=>{}) => {
    if (membership) return ack({ok:false,error:'すでに接続中です'});
    const room = safe(data?.room, 40), token = safe(data?.token, 100), role = data?.role;
    const name = safe(data?.name, 40) || (role === 'camera' ? 'カメラ' : 'モニター');
    if (!/^[a-zA-Z0-9_-]{6,40}$/.test(room) || token.length < 20 || !['camera','monitor'].includes(role))
      return ack({ok:false,error:'ペアリング情報が正しくありません'});
    let r = rooms.get(room);
    if (!r) { if (role !== 'camera') return ack({ok:false,error:'カメラ端末がまだ接続していません'}); r={token,peers:new Map(),created:Date.now()}; rooms.set(room,r); }
    if (r.token !== token) return ack({ok:false,error:'ペアリングキーが一致しません'});
    if (r.peers.size >= 2 || [...r.peers.values()].some(p=>p.role===role)) return ack({ok:false,error:'このペアリングは使用中です'});
    const hadPeer = r.peers.size > 0;
    membership = {room,role}; r.peers.set(socket.id,{socket,role,name});
    socket.join(room);
    ack({ok:true,role,name});
    if (hadPeer) socket.emit('signal',{type:'peer-joined',role:'peer'});
    sendRoom(room,socket.id,{type:'peer-joined',role,name});
  });
  socket.on('signal', data => {
    if (!membership || !data || !['offer','answer','ice','hangup'].includes(data.type)) return;
    const r=rooms.get(membership.room); if(!r || r.token !== safe(data.token,100)) return;
    sendRoom(membership.room,socket.id,{type:data.type,description:data.description,candidate:data.candidate});
  });
  socket.on('disconnect',()=>{
    if(!membership)return;
    const {room}=membership, r=rooms.get(room); if(!r)return;
    r.peers.delete(socket.id); sendRoom(room,socket.id,{type:'peer-left'});
    if(!r.peers.size) rooms.delete(room);
  });
});
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log(`LINKCAM listening on ${PORT}`));
