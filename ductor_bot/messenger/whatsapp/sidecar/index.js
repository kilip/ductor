const { makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } = require('@whiskeysockets/baileys');
const readline = require('readline');
const qrcode = require('qrcode-terminal');
const fs = require('fs');

const authPath = process.argv[2] || './auth_info';
const mode = process.argv[3] || 'daemon'; // 'daemon' or 'auth' or 'logout'

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
});

async function startSock() {
    const { state, saveCreds } = await useMultiFileAuthState(authPath);
    const { version, isLatest } = await fetchLatestBaileysVersion();
    
    const sock = makeWASocket({
        version,
        auth: state,
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;
        
        if (mode === 'auth' && qr) {
            qrcode.generate(qr, { small: true });
            console.log(JSON.stringify({ type: 'qr', status: 'printed' }));
        }

        if (connection === 'close') {
            const shouldReconnect = lastDisconnect.error?.output?.statusCode !== DisconnectReason.loggedOut;
            if (shouldReconnect) {
                startSock();
            } else {
                console.log(JSON.stringify({ type: 'connection', status: 'logged_out' }));
                if (mode === 'auth') process.exit(0);
            }
        } else if (connection === 'open') {
            const myJid = sock.user.id.split(':')[0] + '@s.whatsapp.net';
            console.log(JSON.stringify({ type: 'connection', status: 'connected', myJid: myJid, user: sock.user }));
            if (mode === 'auth') {
                process.exit(0);
            }
        }
    });

    sock.ev.on('messages.upsert', async m => {
        if (mode !== 'daemon') return;
        if (m.type !== 'notify') return;
        for (const msg of m.messages) {
            console.log(JSON.stringify({ type: 'message', message: msg }));
        }
    });

    rl.on('line', async (line) => {
        if (!line.trim()) return;
        try {
            const cmd = JSON.parse(line);
            if (cmd.action === 'send') {
                await sock.sendPresenceUpdate('paused', cmd.jid);
                await sock.sendMessage(cmd.jid, { text: cmd.text });
                console.log(JSON.stringify({ type: 'ack', id: cmd.id }));
            } else if (cmd.action === 'typing') {
                await sock.sendPresenceUpdate('composing', cmd.jid);
            }
        } catch (e) {
            console.error(JSON.stringify({ type: 'error', error: e.message }));
        }
    });
}

if (mode === 'logout') {
    if (fs.existsSync(authPath)) {
        fs.rmSync(authPath, { recursive: true, force: true });
    }
    console.log(JSON.stringify({ type: 'logout', status: 'success' }));
    process.exit(0);
} else {
    startSock();
}
