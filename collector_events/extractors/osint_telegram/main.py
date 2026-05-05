from __future__ import annotations
import asyncio
import logging
import os
import sys
from pathlib import Path

# Adiciona o root do projeto ao sys.path para garantir que forex_shared seja encontrado
SERVICE_ROOT = Path(__file__).resolve().parents[3] # collector_events/collector_events/extractors/osint_telegram -> collector_events -> collector_events -> services -> collector_events
REPO_ROOT = Path(__file__).resolve().parents[5] # C:\Projects\forex_system

if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from forex_shared.env_config_manager import EnvConfigManager
from forex_shared.logging.get_logger import setup_logging, get_logger
from collector_events.extractors.osint_telegram.worker import OsintTelegramWorker

setup_logging(level=logging.INFO)
log = get_logger("OsintBootstrapper")

async def run_bootstrapper():
    """
    Bootstrapper para inicializar o OsintTelegramWorker e criar uma sessão via comando.
    """
    log.info("Iniciando Bootstrapper de OSINT Telegram...")
    
    # 1. Inicializa Configuração
    try:
        EnvConfigManager.startup()
    except Exception as e:
        log.error(f"Falha ao carregar EnvConfig: {e}")
        return

    # 2. Inicializa o Worker
    worker = OsintTelegramWorker(worker_id="osint-worker-01")
    await worker.on_startup()

    # 3. Define a configuração da sessão
    # Usando caminhos relativos ao root do pacote collector_events
    package_root = Path(__file__).resolve().parents[2] # .../extractors/osint_telegram -> .../extractors -> .../collector_events
    channels_file = package_root / "sources" / "telegram_channels.json"
    
    session_config = {
        "session_id": "default",
        "api_id": 24274368,
        "channels_file": str(channels_file) if channels_file.exists() else None,
        "channel_set": "all",
        "backfill_days": 1,
        "max_messages_limit": 100, # Teste com limite de 100 mensagens
        "log_to_file": True,
        "backfill_interval": 30.0
    }

    # 4. Verifica se a sessão já existe no worker (idempotência)
    if session_config["session_id"] in worker.sessions:
        log.info(f"Sessão '{session_config['session_id']}' já está em execução.")
    else:
        log.info(f"Enviando comando SESSION_CREATE para sessão '{session_config['session_id']}'")
        # Simula o recebimento de um comando via Broker (ou pode usar worker.handle_command se quiser bypass MQ)
        command = {
            "command": "SESSION_CREATE",
            "session_id": session_config["session_id"],
            "config": session_config
        }
        await worker.handle_command(command)

    log.info("Worker aguardando processamento... (Limite de 100 mensagens configurado)")
    
    try:
        while True:
            active_sessions = {sid: s for sid, s in worker.sessions.items() if s.status == "RUNNING"}
            if not active_sessions:
                log.info("Nenhuma sessão ativa (todas STOPPED). Encerrando bootstrapper...")
                break
                
            for sid, session in active_sessions.items():
                log.info(f"Status [{sid}]: Recv={session.received_count}, Accepted={session.accepted_count}, Limit={session.processed_messages_count}/{session.max_messages_limit}")
            
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        log.info("Interrompido pelo usuário.")
    finally:
        await worker.on_shutdown()
        log.info("Bootstrapper encerrado.")

if __name__ == "__main__":
    asyncio.run(run_bootstrapper())
