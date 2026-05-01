import asyncio
import json
import logging
from dataclasses import asdict

from forex_shared.env_config_manager import EnvConfigManager
EnvConfigManager.startup() 

from collector_events.globalintel.orchestrator import IntelOrchestrator
from collector_events.globalintel.extractor_factory import ScheduleEntry

# Ajuste o import abaixo para o caminho correto onde você salvou o test_extractor.py
from collector_events.globalintel.extractors.test_extractor import TestExtractor 

class TestOrchestrator(IntelOrchestrator):
    def __init__(self, input_filepath: str, output_filepath: str, interval_seconds: int = 1):
        # Inicia a IA e o Scheduler através da classe pai
        super().__init__()
        
        self.output_filepath = output_filepath
        self.enriched_results = []
        
        # 1. SUBSTITUI O AGENDAMENTO PADRÃO
        # Ignora os 40 extratores normais e agenda apenas o nosso TestExtractor
        self.test_extractor = TestExtractor(filepath=input_filepath)
        self._schedule = [
            ScheduleEntry(extractor=self.test_extractor, interval_seconds=interval_seconds)
        ]
        self.total_items = len(self.test_extractor.items_data)

    async def _enrich_and_store(self, result, extractor):
        """
        INTERCEPTAÇÃO:
        Chamamos o processamento real de NLP na classe pai e, em seguida,
        desviamos uma cópia dos dados para o nosso arquivo JSON local.
        """
        # Executa o EventProcessor (FinBERT, DeBERTa, GLiNER) e salva no Redis
        await super()._enrich_and_store(result, extractor)

        # Captura os dados assim que a IA termina de enriquecer
        if result.items:
            for item in result.items:
                # Converte o dataclass de volta para um dicionário normal
                self.enriched_results.append(asdict(item))
            
            # Grava progressivamente no disco (para podermos ver em tempo real)
            with open(self.output_filepath, "w", encoding="utf-8") as f:
                json.dump(self.enriched_results, f, ensure_ascii=False, indent=4)
                
            self.log.info(f"💾 Interceptado e gravado: {len(self.enriched_results)}/{self.total_items} no arquivo {self.output_filepath}")
            
            # Auto-desligamento quando finalizar a carga de teste
            if len(self.enriched_results) >= self.total_items:
                self.log.info("🎉 Todos os itens processados. Encerrando o Orquestrador de Teste.")
                self.stop_scheduler()

async def run_test_pipeline():
    # Configurações do Teste
    INPUT_JSON = "mock_intel_items_big.json"
    OUTPUT_JSON = "mock_intel_items_big_process_result.json"
    INTERVALO_SEGUNDOS = 1  # Dispara a cada 1 segundo

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    print(f"\n🚀 Iniciando Pipeline de NLP Isolada (Intervalo: {INTERVALO_SEGUNDOS}s)...\n")

    orch = TestOrchestrator(
        input_filepath=INPUT_JSON, 
        output_filepath=OUTPUT_JSON, 
        interval_seconds=INTERVALO_SEGUNDOS
    )
    
    if orch.total_items == 0:
        print("❌ Nenhum item para processar. O arquivo de mock existe?")
        return

    # Inicia o loop assíncrono do APScheduler
    await orch.start_scheduler()

    # Mantém o script vivo até que a classe chame o 'stop_scheduler()'
    while orch._scheduler.running:
        await asyncio.sleep(0.5)

    print(f"\n✅ Teste concluído com sucesso. Verifique o arquivo: {OUTPUT_JSON}")

if __name__ == "__main__":
    asyncio.run(run_test_pipeline())