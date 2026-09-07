const { WebSocketServer } = require('ws');
const { Innertube } = require('youtubei.js');

// Railway/Render inyectan el puerto en esta variable; localmente usamos 8080.
const PORT = process.env.PORT || 8080;

const wss = new WebSocketServer({ port: PORT });
console.log(`Iniciando servidor WebSocket en el puerto ${PORT}...`);

// ── Cliente de YouTube ───────────────────────────────────────────────
// Una sola instancia de Innertube para todo el servidor. Esto es seguro
// de compartir: Innertube.getInfo() no guarda estado por usuario, solo
// hace peticiones HTTP y devuelve el resultado. Cada usuario recibe su
// propio objeto `livechat` a partir de aquí, así que sus mensajes nunca
// se mezclan entre sí.
let yt = null;
let ytReadyPromise = null;

function getYouTubeClient() {
  if (!ytReadyPromise) {
    ytReadyPromise = Innertube.create()
      .then((youtube) => {
        yt = youtube;
        console.log('YouTubei.js listo.');
        return youtube;
      })
      .catch((err) => {
        console.error('Error al inicializar YouTubei:', err.message);
        ytReadyPromise = null; // permite reintentar en la próxima conexión
        throw err;
      });
  }
  return ytReadyPromise;
}
getYouTubeClient(); // arrancar la inicialización desde ya, sin esperar al primer cliente

// ── Constantes de configuración ─────────────────────────────────────
const DEDUPE_WINDOW_MS = 2000; // ventana para descartar mensajes duplicados de YouTube
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_DELAY_MS = 2000; // backoff exponencial: 2s, 4s, 8s, 16s, 32s

function safeSend(ws, payload) {
  // El socket puede haberse cerrado entre el evento y el envío (p. ej. el
  // usuario cerró TurboWarp justo cuando llegaba un mensaje del chat).
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

// ── Manejo de una sesión individual (una pestaña/proyecto de TurboWarp) ──
function createSession(ws) {
  let livechat = null;
  let currentVideoId = null;
  let reconnectAttempts = 0;
  let reconnectTimer = null;
  let stopped = false; // true cuando el usuario cambia de video o cierra la conexión

  const recentMessages = new Map(); // huella del mensaje -> timeoutId, para poder limpiar todo en stopLiveChat()

  function clearDedupeCache() {
    for (const timeoutId of recentMessages.values()) {
      clearTimeout(timeoutId);
    }
    recentMessages.clear();
  }

  function isDuplicate(hash) {
    if (recentMessages.has(hash)) return true;
    const timeoutId = setTimeout(() => recentMessages.delete(hash), DEDUPE_WINDOW_MS);
    recentMessages.set(hash, timeoutId);
    return false;
  }

  function stopLiveChat() {
    stopped = true;
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (livechat) {
      try {
        livechat.stop();
      } catch (err) {
        // El chat ya pudo haberse detenido solo (p. ej. el live terminó);
        // no es un error real para el usuario, solo lo dejamos en el log.
        console.warn('Aviso al detener livechat:', err.message);
      }
      livechat = null;
    }
    clearDedupeCache();
  }

  async function startLiveChat(videoId) {
    stopLiveChat();
    stopped = false;
    currentVideoId = videoId;
    reconnectAttempts = 0;

    await connectLiveChat();
  }

  async function connectLiveChat() {
    if (stopped) return;

    try {
      console.log(`[DEBUG] Obteniendo cliente de YouTube para videoId="${currentVideoId}"...`);
      const client = yt || (await getYouTubeClient());
      console.log(`[DEBUG] Cliente listo. Llamando a client.getInfo("${currentVideoId}")...`);
      const info = await client.getInfo(currentVideoId);

      const isLiveNow = info?.basic_info?.is_live;
      console.log(`[DEBUG] getInfo respondió. Título: "${info?.basic_info?.title || 'desconocido'}" | is_live: ${isLiveNow}`);

      if (isLiveNow === false) {
        // Hipótesis a confirmar: getLiveChat() no lanza error con un video
        // que no está en vivo, solo devuelve un objeto que nunca emitirá
        // 'chat-update'. Cortamos aquí para dar un mensaje claro en vez de
        // silencio total.
        console.warn(`[DEBUG] El video "${currentVideoId}" NO está en vivo ahora mismo (is_live=false). No se puede escuchar su chat.`);
        safeSend(ws, { type: 'STATUS', data: { state: 'NOT_LIVE', videoId: currentVideoId } });
        return;
      }

      livechat = info.getLiveChat();
      console.log('[DEBUG] getLiveChat() devolvió un objeto. Registrando listeners y arrancando...');

      livechat.on('chat-update', (action) => handleChatUpdate(action));

      livechat.on('error', (err) => {
        console.error(`Error en livechat (${currentVideoId}):`, err?.message || err);
        scheduleReconnect();
      });

      livechat.on('end', () => {
        console.log(`El chat de ${currentVideoId} terminó (live finalizado).`);
        safeSend(ws, { type: 'STATUS', data: { state: 'ENDED', videoId: currentVideoId } });
        stopLiveChat();
      });

      livechat.start();
      reconnectAttempts = 0; // conexión exitosa, resetear el contador de reintentos
      console.log(`Conectado al chat de: ${currentVideoId}`);
      safeSend(ws, { type: 'STATUS', data: { state: 'CONNECTED', videoId: currentVideoId } });
    } catch (error) {
      console.error(`Error al conectar con el chat (${currentVideoId}):`, error.message);
      scheduleReconnect();
    }
  }

  function scheduleReconnect() {
    if (stopped) return;
    if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
      console.error(`Máximo de reintentos alcanzado para ${currentVideoId}.`);
      safeSend(ws, { type: 'STATUS', data: { state: 'FAILED', videoId: currentVideoId } });
      return;
    }
    const delay = RECONNECT_BASE_DELAY_MS * Math.pow(2, reconnectAttempts);
    reconnectAttempts++;
    console.log(`Reintentando conexión a ${currentVideoId} en ${delay}ms (intento ${reconnectAttempts})...`);
    reconnectTimer = setTimeout(connectLiveChat, delay);
  }

  function handleChatUpdate(action) {
    const item = action.item;
    console.log(`[DEBUG] chat-update recibido. item?.type = ${item?.type || '(sin item)'}`);
    if (!item) return;

    switch (item.type) {
      case 'LiveChatTextMessage':
        return handleTextMessage(item);
      case 'LiveChatPaidMessage':
        return handlePaidMessage(item);
      case 'LiveChatPaidSticker':
        return handlePaidSticker(item);
      case 'LiveChatMembershipItem':
        return handleMembership(item);
      default:
        return; // otros tipos internos de YouTube que no exponemos por ahora
    }
  }

  function getAuthorFields(author) {
    const avatarList = author?.thumbnails;
    return {
      author: author?.name?.toString() || 'Desconocido',
      avatar: avatarList && avatarList.length > 0 ? avatarList[0].url : '',
      isMod: author?.is_moderator || false,
      isOwner: author?.is_chat_owner || false,
      isVerified: author?.is_verified || false,
      isMember: author?.is_member || false,
    };
  }

  function handleTextMessage(item) {
    const authorFields = getAuthorFields(item.author);
    const msgText = item.message?.toString() || '';
    const msgHash = `${authorFields.author}:${msgText}`;

    if (isDuplicate(msgHash)) return;

    safeSend(ws, {
      type: 'COMMENT',
      data: { ...authorFields, message: msgText },
    });
  }

  function handlePaidMessage(item) {
    // Super Chat: mensaje pagado, con monto y color asignado por YouTube según el nivel.
    // purchase_amount ya viene como string (p. ej. "$5.00"), no como objeto Text.
    const authorFields = getAuthorFields(item.author);
    safeSend(ws, {
      type: 'SUPERCHAT',
      data: {
        ...authorFields,
        message: item.message?.toString() || '',
        amount: item.purchase_amount || '',
      },
    });
  }

  function handlePaidSticker(item) {
    // Super Sticker: como el Super Chat pero con una imagen en vez de texto.
    // Nota: a diferencia de LiveChatPaidMessage (confirmado como string),
    // no verifiqué la forma exacta de purchase_amount ni de sticker en esta
    // clase, así que uso encadenamiento opcional por seguridad. Si el monto
    // o la imagen no llegan, revisa esto con un console.log(item) primero.
    const authorFields = getAuthorFields(item.author);
    safeSend(ws, {
      type: 'SUPERSTICKER',
      data: {
        ...authorFields,
        amount: item.purchase_amount?.toString?.() || item.purchase_amount || '',
        stickerUrl: item.sticker?.[0]?.url || item.sticker?.url || '',
      },
    });
  }

  function handleMembership(item) {
    // Nueva membresía o renovación. header_primary_text trae el anuncio
    // ("Bienvenido a la membresía" / "X lleva N meses"), header_subtext el detalle.
    const authorFields = getAuthorFields(item.author);
    safeSend(ws, {
      type: 'MEMBERSHIP',
      data: {
        ...authorFields,
        headline: item.header_primary_text?.toString() || '',
        detail: item.header_subtext?.toString() || '',
      },
    });
  }

  return {
    handleMessage(raw) {
      console.log('[DEBUG] Mensaje crudo recibido:', raw.toString());

      let payload;
      try {
        payload = JSON.parse(raw);
      } catch (error) {
        console.error('Mensaje inválido recibido (no es JSON):', error.message);
        return;
      }

      console.log('[DEBUG] Payload parseado:', JSON.stringify(payload));

      if (payload.action === 'START' && payload.videoId) {
        console.log(`[DEBUG] Acción START reconocida, videoId="${payload.videoId}". Llamando a startLiveChat...`);
        startLiveChat(payload.videoId).catch((err) => {
          console.error('Error inesperado iniciando el chat:', err.message);
        });
      } else if (payload.action === 'STOP') {
        stopLiveChat();
        safeSend(ws, { type: 'STATUS', data: { state: 'STOPPED', videoId: currentVideoId } });
      } else {
        console.warn('[DEBUG] Mensaje no reconocido. payload.action =', payload.action, '| payload.videoId =', payload.videoId);
      }
    },
    handleClose() {
      stopLiveChat();
    },
  };
}

// ── Conexión de cada cliente (cada usuario de TurboWarp) ────────────
wss.on('connection', (ws) => {
  console.log('¡Extensión conectada!');
  const session = createSession(ws);

  ws.on('message', (message) => session.handleMessage(message));

  ws.on('close', () => {
    console.log('TurboWarp desconectado.');
    session.handleClose();
  });

  ws.on('error', (err) => {
    console.error('Error de WebSocket:', err.message);
  });
});

// Evitar que errores no controlados tumben el proceso completo del servidor
// (y con él, a todos los usuarios conectados al mismo tiempo).
process.on('unhandledRejection', (reason) => {
  console.error('Promesa rechazada sin manejar:', reason);
});
