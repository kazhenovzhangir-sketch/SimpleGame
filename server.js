// Сервер комнат + раздача игры. Запуск: npm install && npm start
const http=require('http'),fs=require('fs'),path=require('path');
const {WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000;
const srv=http.createServer((q,r)=>{
 if(q.url==='/health'){r.end('ok');return}
 r.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});
 r.end(fs.readFileSync(path.join(__dirname,'index.html')));
});
const wss=new WebSocketServer({server:srv,maxPayload:8192});
const rooms=new Map();let nid=0;
const newId=()=>Math.random().toString(36).slice(2,9)+(nid++).toString(36);
const list=n=>[...(rooms.get(n)||[])].map(c=>({peer:c.id,presence:c.pres[n]||{}}));
function bcast(n,msg,except){const s=rooms.get(n);if(!s)return;const m=JSON.stringify(msg);for(const c of s)if(c!==except&&c.readyState===1)c.send(m)}
const sendPeers=(n,joined=[],left=[])=>bcast(n,{t:'peers',room:n,list:list(n),joined,left});
function leaveRoom(c,n){
 const s=rooms.get(n);if(!s||!s.delete(c))return;
 delete c.pres[n];c.rooms.delete(n);
 if(!s.size)rooms.delete(n);else sendPeers(n,[],[c.id]);
}
wss.on('connection',ws=>{
 ws.id=newId();ws.rooms=new Set();ws.pres={};ws.cnt=0;ws.win=Date.now();ws.alive=true;
 ws.send(JSON.stringify({t:'hello',id:ws.id}));
 ws.on('pong',()=>{ws.alive=true});
 ws.on('message',raw=>{
  const now=Date.now();if(now-ws.win>1000){ws.win=now;ws.cnt=0}if(++ws.cnt>80)return;     // защита от спама
  let m;try{m=JSON.parse(raw)}catch(e){return}
  const room=String(m.room||'');if(!/^[a-z0-9_]{1,24}$/.test(room))return;
  if(m.t==='join'){
   if(ws.rooms.has(room))return;
   const s=rooms.get(room)||new Set();
   if(room!=='_lobby'&&s.size>=8){ws.send(JSON.stringify({t:'full',room}));return}
   rooms.set(room,s);s.add(ws);ws.rooms.add(room);ws.pres[room]={};sendPeers(room,[ws.id],[]);
  }else if(m.t==='leave'){leaveRoom(ws,room)}
  else if(!ws.rooms.has(room)){return}
  else if(m.t==='emit'){
   const tp=String(m.topic||'');if(!/^[a-z0-9._-]{1,48}$/.test(tp))return;
   if(JSON.stringify(m.data===undefined?null:m.data).length>4096)return;
   bcast(room,{t:'ev',room,topic:tp,peer:ws.id,data:m.data},ws);
  }else if(m.t==='pres'&&m.patch&&typeof m.patch==='object'){
   const p=ws.pres[room];
   for(const k of Object.keys(m.patch).slice(0,16)){
    if(!/^[a-z0-9_]{1,24}$/.test(k))continue;const v=m.patch[k];
    if(v===null)delete p[k];else if(JSON.stringify(v).length<200)p[k]=v;
   }
   sendPeers(room);
  }
 });
 ws.on('close',()=>{for(const n of [...ws.rooms])leaveRoom(ws,n)});
 ws.on('error',()=>{});
});
setInterval(()=>wss.clients.forEach(c=>{if(!c.alive)return c.terminate();c.alive=false;c.ping()}),30000);
srv.listen(PORT,()=>console.log('Сервер запущен, порт',PORT));
