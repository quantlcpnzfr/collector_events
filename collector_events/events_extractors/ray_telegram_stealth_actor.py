import ray
import asyncio
import logging
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from datetime import datetime

@ray.remote(max_restarts=5, max_task_retries=-1)
class StealthTelegramActor:
    def __init__(self, feed_type: str, config: dict, api_id: int, api_hash: str):
        self.feed_type = feed_type
        self.channels = config.get("channels", [])
        self.logger = logging.getLogger(f"StealthActor-{feed_type}")
        
        # 🛡️ STEALTH MODE: Falsificando a assinatura do dispositivo
        # Nunca usamos as configurações padrão para evitar bans instantâneos
        self.client = TelegramClient(
            f'sessions/session_{self.feed_type}', # Mantém a sessão salva
            api_id,
            api_hash,
            device_model="Samsung Galaxy S23 Ultra", # Mascara como um mobile premium
            system_version="Android 14",
            app_version="10.6.1", # Versão de um app oficial recente
            lang_code="en",
            system_lang_code="en-US"
        )

    async def _process_message(self, event):
        """Lógica executada no exato milissegundo que a notícia sai."""
        chat = await event.get_chat()
        channel_name = getattr(chat, 'username', str(chat.id))
        
        data = {
            "timestamp": event.date.isoformat(),
            "feed_type": self.feed_type,
            "channel": f"@{channel_name}",
            "text": event.raw_text
        }
        
        self.logger.info(f"[{self.feed_type}] ⚡ Notícia capturada de @{channel_name}")
        
        # TODO: Enviar assincronamente para o RabbitMQ/Redis
        # await self.broker.publish(data, routing_key=f"intel.{self.feed_type}")

    async def run(self):
        """Inicia a escuta passiva invisível usando WebSockets."""
        self.logger.info(f"A iniciar Stealth Actor para: {self.feed_type}")
        
        try:
            # Inicia o cliente. Se não houver ficheiro de sessão (.session), 
            # ele pedirá o código OTP na primeira vez que rodar.
            await self.client.start()
            
            # O Telethon lida com a concorrência internamente. 
            # Em vez de criarmos vários loops `while`, registamos um Listener (Pub/Sub)
            # que reage passivamente (invisível ao anti-spam) apenas aos canais que queremos.
            @self.client.on(events.NewMessage(chats=self.channels))
            async def handler(event):
                await self._process_message(event)

            self.logger.info(f"[{self.feed_type}] Escuta passiva ativada para {len(self.channels)} canais.")
            
            # Mantém este Actor vivo eternamente no event loop
            await self.client.run_until_disconnected()
            
        except FloodWaitError as e:
            self.logger.critical(f"[{self.feed_type}] Limite do Telegram atingido! Pausa de {e.seconds}s.")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            self.logger.error(f"Erro fatal no actor {self.feed_type}: {e}")
            raise e # O Ray irá capturar o erro e reiniciar o Actor automaticamente