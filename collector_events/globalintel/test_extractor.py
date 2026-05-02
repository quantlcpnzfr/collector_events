import json
import os
from pathlib import Path
from typing import List

import aiohttp
from collector_events.globalintel.config import _CONFIG_DIR
from forex_shared.domain.intel import IntelItem
from collector_events.globalintel.base import BaseExtractor

class TestExtractor(BaseExtractor):
    DOMAIN = "test"
    SOURCE = "mock_file"
    REDIS_KEY = "test:mock:v1"
    TTL_SECONDS = 3600  # Não importa muito para o teste
    
    _CURRENT_DIR = Path(__file__).parent
    _MOCK_FILE = _CURRENT_DIR / "mock_intel_items_big.json"

    def __init__(self, filepath: str | Path = _MOCK_FILE):
        super().__init__()
        self.filepath = filepath
        self.items_data: List[IntelItem] = []
        self.current_index = 0
        self._load_data()

    def _load_data(self):
        """Carrega os dados do arquivo JSON local para a memória na inicialização."""
        if not os.path.exists(self.filepath):
            self.log.error(f"Arquivo de mock não encontrado: {self.filepath}")
            return

        try: 
            with open(self.filepath, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
                # Instancia os IntelItems exatamente como a base espera
                self.items_data = [IntelItem(**item) for item in raw_data]
                self.log.info(f"Mock carregado com {len(self.items_data)} itens.")
        except Exception as e:
            self.log.error(f"Erro ao carregar o arquivo de mock: {e}")

    async def _fetch(self, session: aiohttp.ClientSession) -> List[IntelItem]:
        """
        O Orquestrador chamará este método a cada N segundos.
        Retornamos apenas 1 item por chamada, simulando o delay de uma rede real.
        """
        if self.current_index < len(self.items_data):
            item = self.items_data[self.current_index]
            self.current_index += 1
            
            self.log.info(f"📡 TestExtractor extraindo item {self.current_index}/{len(self.items_data)}: {item.title[:40]}...")
            return [item]
        else:
            self.log.info("✅ TestExtractor esgotou todos os itens do mock.")
            return []