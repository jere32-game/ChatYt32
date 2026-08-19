const { Innertube } = require('youtubei.js');

// Guardamos 'yt' fuera de la función para aprovechar el "Warm Boot" de Vercel
// Esto hace que las consultas siguientes sean mucho más rápidas.
let yt = null; 

export default async function handler(req, res) {
    const videoId = req.query.id;

    if (!videoId) {
        return res.status(400).json({ success: false, error: 'Falta el ID del video' });
    }

    try {
        if (!yt) {
            yt = await Innertube.create();
        }

        const info = await yt.getInfo(videoId);
        const livechat = info.getLiveChat();

        let messages = [];

        // Encendemos el radar por un tiempo muy corto
        livechat.on('chat-update', (action) => {
            const item = action.item;
            if (item && (item.type === 'LiveChatTextMessage' || item.type === 'LiveChatPaidMessage' || item.type === 'LiveChatMembershipItem')) {
                messages.push({
                    author: item.author?.name || 'Desconocido',
                    message: item.message?.toString() || item.header?.primary_text?.toString() || '',
                    avatar: item.author?.thumbnails?.[0]?.url || '',
                    isMod: item.author?.is_moderator || false,
                    isOwner: item.author?.is_channel_owner || false
                });
            }
        });

        livechat.start();

        // Obligamos a Vercel a esperar 1.5 segundos recolectando comentarios
        await new Promise(resolve => setTimeout(resolve, 1500));

        livechat.stop(); // Apagamos el recolector para que Vercel no crashee

        // Tomamos el último mensaje recolectado (si es que alguien comentó en ese segundo y medio)
        const latestMessage = messages.length > 0 ? messages[messages.length - 1] : null;

        res.status(200).json({ success: true, data: latestMessage });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
}