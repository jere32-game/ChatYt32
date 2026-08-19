const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { Innertube } = require('youtubei.js');

// Configuramos el servidor HTTP para que puedas "verlo" en el navegador
// Esto también evita que el servidor se apague al subirlo a servicios en la nube
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.get('/', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head><title>Servidor YT Live</title></head>
        <body style="background-color: #1a1a1a; color: #00ff00; font-family: monospace; padding: 20px;">
            <h1>🟢 Servidor Activo</h1>
            <p>El backend está corriendo y esperando la conexión de TurboWarp.</p>
        </body>
        </html>
    `);
});

wss.on('connection', (ws) => {
    console.log('¡TurboWarp conectado!');
    let livechat = null;

    ws.on('message', async (message) => {
        const data = JSON.parse(message);
        
        if (data.action === 'START') {
            try {
                const yt = await Innertube.create();
                const info = await yt.getInfo(data.videoId);
                
                livechat = info.getLiveChat();
                
                livechat.on('chat-update', (action) => {
                    const item = action.item;
                    if (!item) return;

                    // Ahora capturamos mensajes normales, SuperChats y mensajes de miembros
                    if (item.type === 'LiveChatTextMessage' || item.type === 'LiveChatPaidMessage' || item.type === 'LiveChatMembershipItem') {
                        
                        // Extraemos la información
                        const authorName = item.author?.name || 'Desconocido';
                        // Dependiendo del tipo de mensaje, el texto está en distintos lugares
                        const textMessage = item.message?.toString() || item.header?.primary_text?.toString() || '';
                        const avatar = item.author?.thumbnails?.[0]?.url || '';
                        
                        // Verificamos rangos
                        const isMod = item.author?.is_moderator || false;
                        const isOwner = item.author?.is_channel_owner || false;

                        // Lo enviamos TODO en un solo paquete
                        ws.send(JSON.stringify({
                            type: 'COMMENT',
                            data: {
                                author: authorName,
                                message: textMessage,
                                avatar: avatar,
                                isMod: isMod,
                                isOwner: isOwner
                            }
                        }));
                    }
                });

                livechat.start();
                console.log(`Escuchando el chat del video ID: ${data.videoId}`);
            } catch (error) {
                console.error('Error al conectar con el Live:', error.message);
            }
        }
    });

    ws.on('close', () => {
        if (livechat) livechat.stop();
        console.log('TurboWarp desconectado');
    });
});

// Iniciamos el servidor dual (HTTP + WS)
const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
    console.log(\`Servidor escuchando en el puerto \${PORT}\`);
    console.log(\`Abre http://localhost:\${PORT} en tu navegador para ver el estado.\`);
});