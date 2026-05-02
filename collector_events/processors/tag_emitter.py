# services/collector_events/collector_events/processors/tag_emitter.py
import logging
from datetime import datetime, timezone, timedelta
from forex_shared.domain.intel import GlobalTag, IntelItem
from forex_shared.providers.mq.rabbitmq_provider_async import MQProviderAsync # Seu provider de MQimpor
import json


log = logging.getLogger("GlobalTagEmitter")

_CONFIG_DIR = Path(__file__).parent / "config"
_PREDICTION_FILE = _CONFIG_DIR / "prediction_assets.json"

class GlobalTagEmitter:
    def __init__(self, mq_provider: MQProviderAsync, prediction_file: str | Path = _PREDICTION_FILE):
        self.mq = mq_provider
        self.threshold = 0.75 # Score mínimo para gerar uma Tag global
        
        # 1. Carrega o roteador de ativos do JSON
        try:
            with open(prediction_file, "r", encoding="utf-8") as f:
                self.prediction_tags = json.load(f)
            log.info(f"Roteador de ativos carregado: {len(self.prediction_tags)} categorias.")
        except Exception as e:
            log.error(f"Falha ao carregar {prediction_file}: {e}")
            self.prediction_tags = {}

    async def emit_if_critical(self, item: IntelItem):
        score = item.extra.get("danger_score", 0.0)
        
        if score >= self.threshold:
            # 1. Determina o Asset (Lógica inicial: mapeia países G10 para moedas)
            # Ex: Se o país for 'US', o asset impactado é 'USD'
            asset = self._predict_financial_asset(item)
            
            # 2. Cria a GlobalTag (O contrato que o Trading Executor espera)
            tag = GlobalTag(
                asset=asset,
                bias="risk_off" if score > 0.85 else "neutral",
                risk_score=score,
                trigger_event_id=item.id,
                established_at=datetime.now(timezone.utc).isoformat(),
                expires_at=(datetime.now(timezone.utc) + timedelta(hours=4)).isoformat(),
                active=True
            )
            
            # 3. Publica no RabbitMQ (Descomentado e configurado)
            try:
                await self.mq.publish(
                    queue_name="intel.global_tags",
                    message=tag.to_mq_payload()
                )
                log.info(f"🚀 GLOBAL TAG EMITIDA -> Asset: {asset} | Score: {score:.2f} | Evento: {item.id}")
            except Exception as e:
                log.error(f"Erro ao publicar GlobalTag no MQ: {e}")
                

    def _predict_financial_asset(self, item: IntelItem) -> str:
        """
        Usa o prediction_tags.json para cruzar os alvos do GLiNER 
        e o texto bruto para prever qual ativo o sistema deve operar.
        """
        # Pega o que o GLiNER achou de alvo estratégico (ex: ["oil refinery"])
        tactical_data = item.extra.get("gliner_tactical", {})
        macro_targets = tactical_data.get("macroeconomic infrastructure or strategic target", [])
        
        text_to_analyze = (item.title + " " + item.body).lower()
        
        # Juntamos os alvos extraídos pela IA com o texto para varredura
        combined_text = text_to_analyze + " " + " ".join(macro_targets).lower()

        # 2. A MÁGICA DO PREDICTION_TAGS.JSON
        # Varre as chaves do seu JSON (XAU, USD, OIL...)
        for asset, keywords in self.prediction_tags.items():
            if asset == "excludeKeywords": 
                continue # Pula a lista de exclusão
                
            # Se alguma palavra do JSON bater com o que a IA extraiu, prevê o ativo!
            if any(kw.lower() in combined_text for kw in keywords):
                return asset 

        # 3. Fallback (Se o JSON não achar nada, vai pela geografia)
        if "US" in item.country: return "USD"
        if "RU" in item.country or "UA" in item.country: return "XAU"
        
        return "GLOBAL"
    