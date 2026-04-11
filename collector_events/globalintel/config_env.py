# services/collector_events/collector_events/globalintel/config_env.py
"""
GlobalIntelConfig — configuração tipada e hot-reloadável para o módulo globalintel.

Centraliza todas as chaves de API, URLs e parâmetros de comportamento dos
extractors. Herda de ``BaseConfig`` (shared_lib) para integração com o
``EnvConfigManager`` (hot-reload via MongoDB + os.environ).

USO DIRETO::

    from collector_events.globalintel.config_env import GlobalIntelConfig

    cfg = GlobalIntelConfig()
    key = cfg.ACLED_API_KEY          # lê os.environ (hot-reloadável)
    cfg.ACLED_API_KEY = "nova_chave" # persiste no MongoDB + atualiza env

COMPATIBILIDADE COM OrchestratorConfig::

    O ``IntelOrchestrator`` aceita ``OrchestratorConfig`` (dataclass legada
    em ``orchestrator.py``) ou ``GlobalIntelConfig`` (nova versão BaseConfig).
    Ambas expõem os mesmos atributos de chave de API.

────────────────────────────────────────────────────────────────────────────
SEGURANÇA: Não logar valores de chaves de API. A sobrescrita de ``to_dict()``
mascara todos os campos sensíveis com "***".
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from forex_shared.config.categories import IntelConfig, StorageConfig


class GlobalIntelConfig(StorageConfig, IntelConfig):
    """Configuração completa para o módulo de inteligência global.

    Todos os campos são hot-reloadáveis via EnvConfigManager.
    Chaves de API são mascaradas em ``to_dict()`` para segurança.
    """

    # ── Campos sensíveis — mascarados em to_dict() ────────────────────
    _SENSITIVE_FIELDS: tuple[str, ...] = (
        "ACLED_API_KEY",
        "ACLED_ACCESS_TOKEN",
        "ACLED_PASSWORD",
        "OTX_API_KEY",
        "ABUSEIPDB_API_KEY",
        "FRED_API_KEY",
        "BLS_API_KEY",
        "EIA_API_KEY",
        "WTO_API_KEY",
        "NASA_FIRMS_KEY",
        "FINNHUB_API_KEY",
        "TELEGRAM_RELAY_SECRET",
        "WINGBITS_API_KEY",
    )

    def to_dict(self) -> dict:
        """Retorna configuração com campos sensíveis mascarados."""
        d = super().to_dict()
        for key in self._SENSITIVE_FIELDS:
            if key in d and d[key]:
                d[key] = "***"
        return d
