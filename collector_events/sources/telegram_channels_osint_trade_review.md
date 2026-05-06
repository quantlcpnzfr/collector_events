# Plano de avaliação e uso dos canais Telegram OSINT / Market Intelligence

Gerado em: 2026-05-06T02:32:14.266734+00:00

Arquivo analisado: `telegram_channels(1).json`  
Fonte declarada no JSON: `worldmonitor/data/telegram-channels.json`  
Observação importante: esta análise foi feita a partir do arquivo fornecido e de conhecimento público/reputacional geral dos canais mais conhecidos. Nesta sessão não houve navegação web externa ativa para abrir cada canal ao vivo.

---

## 1. Veredito geral

A lista está **boa e ambiciosa** como base inicial para um pipeline de trade orientado a eventos, mas não deve entrar inteira com o mesmo peso.

Ela mistura quatro famílias bem diferentes:

1. **OSINT / geopolítica / conflito**: útil para risco macro, petróleo, ouro, USD, JPY, CHF, energia, trigo e risco soberano.
2. **Breaking news / finance tape**: útil para headline risk, macro surprise e reação rápida de mercado.
3. **Cyber intelligence**: útil para risco sistêmico, infraestrutura, tecnologia, cripto e eventos de disrupção.
4. **Forex / crypto signal groups**: útil como `retail_sentiment` ou `contrarian_signal`, não como fonte factual nem sinal automático de execução.

Regra central:

```text
Telegram raw
→ translator
→ language detector
→ entity resolver
→ country resolver
→ source scoring
→ dedupe / cross-source clustering
→ oracle review somente se score >= threshold
```

Nenhuma fonte deve ir direto ao Oracle como verdade. O Oracle deve receber evento enriquecido, normalizado, deduplicado e com score.

---

## 2. Problema encontrado: counts inconsistentes

O JSON declara:

```json
{
  "totalChannelOccurrences": 87,
  "enabled": 87,
  "bySet": {
    "full": 79,
    "tech": 8
  },
  "byTopic": {
    "geopolitics": 11,
    "middleeast": 5,
    "conflict": 7,
    "breaking": 9,
    "osint": 4,
    "cyber": 9
  },
  "byTier": {
    "1": 1,
    "2": 19,
    "3": 25
  },
  "byRegion": {
    "iran": 3,
    "middleeast": 11,
    "global": 26,
    "ukraine": 3,
    "europe": 1,
    "asia": 1
  }
}
```

Mas, ao recalcular diretamente em `channels`, o resultado correto é:

```json
{
  "totalChannelOccurrences": 87,
  "enabled": 87,
  "bySet": {
    "full": 79,
    "tech": 8
  },
  "byTopic": {
    "geopolitics": 18,
    "middleeast": 5,
    "conflict": 19,
    "breaking": 13,
    "osint": 4,
    "cyber": 9,
    "military_intel": 2,
    "finance": 4,
    "markets": 1,
    "forex": 1,
    "crypto": 6,
    "military_alert": 1,
    "forex_signals": 4
  },
  "byTier": {
    "1": 11,
    "2": 44,
    "3": 32
  },
  "byRegion": {
    "iran": 3,
    "middleeast": 17,
    "global": 53,
    "ukraine": 11,
    "europe": 2,
    "asia": 1
  }
}
```

### Script recomendado para recalcular automaticamente

```python
from collections import Counter

def recalc_counts(data: dict) -> dict:
    channels = data.get("channels", [])
    return {
        "totalChannelOccurrences": len(channels),
        "enabled": sum(1 for c in channels if c.get("enabled") is True),
        "bySet": dict(Counter(c.get("channelSet", "unknown") for c in channels)),
        "byTopic": dict(Counter(c.get("topic", "unknown") for c in channels)),
        "byTier": dict(Counter(str(c.get("tier", "unknown")) for c in channels)),
        "byRegion": dict(Counter(c.get("region", "unknown") for c in channels)),
    }

data["counts"] = recalc_counts(data)
```

### Recomendação

Não mantenha `counts` manual. Gere no build, no loader ou num script `validate_telegram_channels.py`.

---

## 3. Distribuição correta atual

### Por channelSet

| channelSet | Count |
|---|---:|
| `full` | 79 |
| `tech` | 8 |

### Por topic

| topic | Count |
|---|---:|
| `breaking` | 13 |
| `conflict` | 19 |
| `crypto` | 6 |
| `cyber` | 9 |
| `finance` | 4 |
| `forex` | 1 |
| `forex_signals` | 4 |
| `geopolitics` | 18 |
| `markets` | 1 |
| `middleeast` | 5 |
| `military_alert` | 1 |
| `military_intel` | 2 |
| `osint` | 4 |

### Por tier

| tier | Count |
|---|---:|
| `1` | 11 |
| `2` | 44 |
| `3` | 32 |

### Por region

| region | Count |
|---|---:|
| `asia` | 1 |
| `europe` | 2 |
| `global` | 53 |
| `iran` | 3 |
| `middleeast` | 17 |
| `ukraine` | 11 |

---

## 4. Classificação operacional recomendada

### P0: ligar primeiro

Canais com maior valor operacional para early signal, geopolítica, headline risk, finance tape ou fluxo cripto relevante.

```text
BNONews
Faytuks
ClashReport
AuroraIntel
OSINTdefender
LiveUAMap
DeepStateUA
kpszsu
operativnoZSU
citeam
IranIntl_En
englishabuali
abualiexpress
VahidOnline
FinancialJuice
firstsquawk
zerohedge
intel_sky
whale_alert_io
binance_announcements
rybar
```

### P1: manter com peso moderado

Boas fontes, mas com mais ruído, viés, duplicação ou necessidade de validação.

Inclui agregadores geopolíticos, regionais, cyber de contexto, marcas que precisam confirmação de autenticidade e canais de mercado menos críticos.

### P2: manter em observação / peso baixo

Canais com alto viés, fontes pouco verificadas, sinais de varejo, pump/crypto, agregadores genéricos ou conteúdo mais útil para narrativa do que para decisão factual.

---

## 5. Ajustes recomendados no schema

Hoje o `decisionUse` está genérico demais em muitos canais. Eu adicionaria estes campos:

```json
{
  "sourceRole": "breaking_news | conflict_osint | regional_news | cyber_intel | finance_tape | forex_retail_signal | crypto_flow | osint_context",
  "biasRisk": "low | medium | high | unknown",
  "verificationRequired": true,
  "confidenceClass": "confirmed_official | known_source | useful_unverified | high_bias | low_signal",
  "canTriggerTrade": false,
  "canInfluenceSentiment": true,
  "sendToTranslator": true,
  "sendToOracleDirectly": false
}
```

### Exemplo para fonte OSINT de conflito

```json
{
  "handle": "ClashReport",
  "sourceRole": "conflict_osint",
  "biasRisk": "medium",
  "verificationRequired": false,
  "confidenceClass": "known_source",
  "sendToTranslator": true,
  "sendToOracleDirectly": false
}
```

### Exemplo para sinal Forex

```json
{
  "handle": "forexsignalfactory",
  "sourceRole": "forex_retail_signal",
  "biasRisk": "high",
  "verificationRequired": true,
  "confidenceClass": "retail_signal_unverified",
  "canTriggerTrade": false,
  "canInfluenceSentiment": true
}
```

---

## 6. Tratamento específico para Forex Signals

Canais como:

```text
forexsignalfactory
altsignals
forexgdp
unitedsignals
prosignalsfx
```

não devem entrar no mesmo pipeline semântico de OSINT geopolítico.

Use um sub-pipeline separado:

```text
telegram.forex_signals.raw
→ parser de sinal
→ normalizador de par / direção / entrada / SL / TP
→ score por histórico real
→ retail_sentiment / contrarian_signal
```

Regras:

```json
{
  "sourceRole": "forex_retail_signal",
  "trustModel": "historical_performance_only",
  "canTriggerTrade": false,
  "canInfluenceSentiment": true,
  "oracleUse": "sentiment_context_only"
}
```

Esses canais podem ser úteis para responder:

- O varejo está comprado ou vendido?
- Existe cluster de sinal em EUR/USD, GBP/USD, XAU/USD?
- Existe excesso de euforia ou medo que possa ser usado como contrarian?

Mas **não** devem acionar trade automaticamente.

---

## 7. Tratamento específico para fontes enviesadas

Alto viés não significa inutilidade. Significa apenas que o conteúdo deve ser interpretado como narrativa, claim ou sinal parcial.

Exemplos de canais que merecem `biasRisk=high` ou validação reforçada:

```text
DDGeopolitics
intel_slava
rybar
milinfolive
FotrosResistancee
RezistanceTrench1
zerohedge
disclosetv
disclosewt
IntelRepublic
thecradlemedia
IranIntl_En
abualiexpress
englishabuali
```

Uso correto:

```text
claim/narrative feed
→ cross-source check
→ cluster com fontes independentes
→ só então vira evento forte
```

---

## 8. Tratamento específico para Cyber

Cyber é relevante para trade quando afeta:

- Bancos, exchanges, infraestrutura de pagamentos.
- Energia, telecom, transporte, governo.
- Grandes empresas listadas.
- Cripto/exchanges/stablecoins.
- Cadeias de suprimento ou risco sistêmico.

Canais cyber devem ir para:

```text
osint.cyber.raw
→ cyber entity resolver
→ affected_sector resolver
→ market impact classifier
```

Não trate todo CVE ou malware como evento de mercado.

---

## 9. Tabela canal a canal

| # | Handle | Label | Topic | Region | Current tier | Suggested priority | Source role | Bias risk | Verify? | Opinion |
|---:|---|---|---|---|---:|---|---|---|---|---|
| 1 | `VahidOnline` | Vahid Online | `geopolitics` | `iran` | 1 | P0 | `geopolitical_or_regional_news` | `medium` | no | Forte para Irã/política interna; bom para risco geopolítico e petróleo. Precisa tradução/normalização. |
| 2 | `abualiexpress` | Abu Ali Express | `middleeast` | `middleeast` | 2 | P0 | `geopolitical_or_regional_news` | `high` | no | Muito útil para Oriente Médio; excelente early signal, mas com viés regional. |
| 3 | `AuroraIntel` | Aurora Intel | `conflict` | `global` | 2 | P0 | `conflict_osint` | `medium` | no | Bom agregador OSINT/conflito global; ótimo para alerta, precisa dedupe. |
| 4 | `BNONews` | BNO News | `breaking` | `global` | 2 | P0 | `breaking_news` | `medium` | no | Boa fonte de breaking news global; útil para headline risk. |
| 5 | `ClashReport` | Clash Report | `conflict` | `global` | 2 | P0 | `conflict_osint` | `medium` | no | Forte para conflito/geopolítica em tempo real; alto valor com validação cruzada. |
| 6 | `DeepStateUA` | DeepState | `conflict` | `ukraine` | 2 | P0 | `conflict_osint` | `medium` | no | Muito relevante para Ucrânia/Rússia; bom para energia europeia, trigo, gás e risco de guerra. |
| 7 | `DefenderDome` | The Defender Dome | `conflict` | `global` | 2 | P1 | `conflict_osint` | `medium` | no | Potencialmente útil para defesa/conflito; validar reputação e qualidade. |
| 8 | `englishabuali` | Abu Ali Express EN | `middleeast` | `middleeast` | 2 | P0 | `geopolitical_or_regional_news` | `high` | no | Versão em inglês do Abu Ali; ótima para pipeline direto e demo. |
| 9 | `IranIntl_En` | Iran International EN | `geopolitics` | `iran` | 2 | P0 | `geopolitical_or_regional_news` | `high` | yes | Fonte relevante para Irã em inglês; bom sinal, mas considerar viés editorial. |
| 10 | `kpszsu` | Air Force of the Armed Forces of Ukraine | `breaking` | `ukraine` | 2 | P0 | `breaking_news` | `medium` | no | Excelente para alertas aéreos/mísseis/drones na Ucrânia; bom risco imediato. |
| 11 | `LiveUAMap` | LiveUAMap | `breaking` | `global` | 2 | P0 | `breaking_news` | `medium` | no | Muito útil para eventos geolocalizados; bom para country/entity resolver. |
| 12 | `OSINTdefender` | OSINTdefender | `conflict` | `global` | 2 | P0 | `conflict_osint` | `medium` | no | Forte para alertas militares/geopolíticos rápidos; precisa validação cruzada. |
| 13 | `OsintUpdates` | Osint Updates | `breaking` | `global` | 2 | P1 | `breaking_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 14 | `bellingcat` | Bellingcat | `osint` | `global` | 3 | P1 | `osint_context_or_aggregator` | `medium` | yes | Fonte investigativa forte; mais contexto/validação do que alerta imediato. |
| 15 | `CyberDetective` | CyberDetective | `cyber` | `global` | 3 | P2 | `cyber_intel` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 16 | `GeopoliticalCenter` | GeopoliticalCenter | `geopolitics` | `global` | 3 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 17 | `Middle_East_Spectator` | Middle East Spectator | `middleeast` | `middleeast` | 3 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 18 | `MiddleEastNow_Breaking` | Middle East Now Breaking | `middleeast` | `middleeast` | 3 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 19 | `nexta_tv` | NEXTA | `geopolitics` | `europe` | 3 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 20 | `OSINTIndustries` | OSINT Industries | `osint` | `global` | 3 | P1 | `osint_context_or_aggregator` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 21 | `Osintlatestnews` | OSIntOps News | `osint` | `global` | 3 | P1 | `osint_context_or_aggregator` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 22 | `osintlive` | OSINT Live | `osint` | `global` | 3 | P1 | `osint_context_or_aggregator` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 23 | `OsintTv` | OsintTV | `geopolitics` | `global` | 3 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 24 | `spectatorindex` | The Spectator Index | `breaking` | `global` | 3 | P1 | `breaking_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 25 | `wfwitness` | Witness | `breaking` | `global` | 3 | P1 | `breaking_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 26 | `war_monitor` | monitor | `breaking` | `ukraine` | 3 | P1 | `breaking_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 27 | `nayaforiraq` | Naya for Iraq | `geopolitics` | `middleeast` | 3 | P2 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 28 | `yediotnews25` | Yedioth News | `breaking` | `middleeast` | 3 | P1 | `breaking_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 29 | `DDGeopolitics` | DD Geopolitics | `geopolitics` | `global` | 3 | P1 | `geopolitical_or_regional_news` | `high` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 30 | `FotrosResistancee` | Fotros Resistance | `conflict` | `iran` | 3 | P2 | `conflict_osint` | `high` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 31 | `RezistanceTrench1` | Resistance Trench | `conflict` | `middleeast` | 3 | P2 | `conflict_osint` | `high` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 32 | `geopolitics_prime` | Geopolitics Prime | `geopolitics` | `global` | 3 | P2 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 33 | `thecradlemedia` | The Cradle | `middleeast` | `middleeast` | 3 | P1 | `geopolitical_or_regional_news` | `high` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 34 | `LebUpdate` | Lebanon Update | `breaking` | `middleeast` | 3 | P1 | `breaking_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 35 | `middleeastobserver` | Middle East Observer | `geopolitics` | `middleeast` | 2 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 36 | `MiddleEastEye_TG` | Middle East Eye | `geopolitics` | `middleeast` | 2 | P1 | `geopolitical_or_regional_news` | `medium` | yes | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 37 | `dragonwatch` | Dragon Watch | `geopolitics` | `asia` | 2 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 38 | `thehackernews` | The Hacker News | `cyber` | `global` | 2 | P1 | `cyber_intel` | `medium` | yes | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 39 | `cybersecboardrm` | Cybersecurity Boardroom | `cyber` | `global` | 3 | P2 | `cyber_intel` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 40 | `securelist` | Securelist by Kaspersky | `cyber` | `global` | 3 | P2 | `cyber_intel` | `medium` | yes | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 41 | `DarkWebInformer` | Dark Web Informer | `cyber` | `global` | 3 | P2 | `cyber_intel` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 42 | `CYBERWARCOM` | CYBERWAR.COM | `cyber` | `global` | 2 | P1 | `cyber_intel` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 43 | `thecyberwire` | The CyberWire | `cyber` | `global` | 2 | P1 | `cyber_intel` | `medium` | yes | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 44 | `vxunderground` | vx-underground | `cyber` | `global` | 2 | P1 | `cyber_intel` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 45 | `falconfeeds` | FalconFeeds.io | `cyber` | `global` | 3 | P1 | `cyber_intel` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 46 | `Faytuks` | Faytuks News | `breaking` | `global` | 1 | P0 | `breaking_news` | `medium` | no | Excelente breaking geopolitical news; P0 para early signal. |
| 47 | `BellumActaNews` | Bellum Acta News | `geopolitics` | `global` | 2 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 48 | `IntelRepublic` | Intel Republic | `geopolitics` | `global` | 2 | P1 | `geopolitical_or_regional_news` | `high` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 49 | `intel_slava` | Intel Slava Z | `conflict` | `ukraine` | 3 | P2 | `conflict_osint` | `high` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 50 | `rybar` | Rybar | `conflict` | `ukraine` | 1 | P0 | `conflict_osint` | `high` | no | Fonte militar russa muito relevante e enviesada; usar como narrativa/claim feed. |
| 51 | `milinfolive` | Military Informant | `conflict` | `ukraine` | 2 | P1 | `conflict_osint` | `high` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 52 | `operativnoZSU` | Operativno ZSU | `conflict` | `ukraine` | 1 | P0 | `conflict_osint` | `medium` | no | Fonte operacional ucraniana; bom para Ucrânia/Rússia, precisa tradução. |
| 53 | `UkraineNow` | Ukraine NOW | `breaking` | `ukraine` | 2 | P1 | `breaking_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 54 | `pravdaGerashchenko_en` | Anton Gerashchenko EN | `geopolitics` | `ukraine` | 2 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 55 | `israelwarroom` | Israel War Room | `conflict` | `middleeast` | 2 | P1 | `conflict_osint` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 56 | `CIG_telegram` | CIG / Conflict Intelligence | `conflict` | `global` | 2 | P1 | `conflict_osint` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 57 | `atlasnewstelegram` | Atlas News | `geopolitics` | `global` | 2 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 58 | `casusbellilive` | Casus Belli | `conflict` | `global` | 2 | P1 | `conflict_osint` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 59 | `CaucasusWar` | Caucasus War Report | `conflict` | `middleeast` | 2 | P1 | `conflict_osint` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 60 | `disclosetv` | Disclose.tv | `breaking` | `global` | 2 | P1 | `breaking_news` | `high` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 61 | `citeam` | Conflict Intelligence Team (CIT) | `military_intel` | `ukraine` | 1 | P0 | `conflict_osint` | `medium` | no | Conflict Intelligence Team; bom valor analítico para Rússia/Ucrânia. |
| 62 | `ukrainewarreport` | Ukraine War Reports | `conflict` | `ukraine` | 2 | P1 | `conflict_osint` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 63 | `syriatracker` | Syria Tracker | `conflict` | `middleeast` | 2 | P1 | `conflict_osint` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 64 | `middleeastmonitor` | Middle East Monitor | `geopolitics` | `middleeast` | 2 | P1 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 65 | `firstsquawk` | First Squawk (Financial Feeds) | `finance` | `global` | 1 | P0 | `finance_market_tape` | `medium` | no | Muito útil para tape/headlines financeiros; ótimo para FX, bonds, commodities. |
| 66 | `tradingview` | TradingView | `markets` | `global` | 2 | P1 | `finance_market_tape` | `medium` | yes | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 67 | `forexsignalfactory` | Forex Signal Factory | `forex` | `global` | 2 | P1 | `forex_retail_signal` | `medium` | no | Usar como retail sentiment/contrarian; não como sinal automático. |
| 68 | `whale_alert_io` | Whale Alert | `crypto` | `global` | 1 | P0 | `crypto_flow_or_crypto_news` | `medium` | yes | Útil para fluxo on-chain/cripto; P0 se houver módulo cripto. |
| 69 | `glassnode` | Glassnode Alerts | `crypto` | `global` | 2 | P1 | `crypto_flow_or_crypto_news` | `medium` | yes | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 70 | `crypto_pump_alerts` | Crypto Pump Alerts | `crypto` | `global` | 3 | P2 | `crypto_flow_or_crypto_news` | `medium` | yes | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 71 | `etherdrops_bot` | EtherDrops (Blockchain Feeds) | `crypto` | `global` | 3 | P2 | `crypto_flow_or_crypto_news` | `medium` | yes | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 72 | `coindesk` | CoinDesk | `crypto` | `global` | 2 | P1 | `crypto_flow_or_crypto_news` | `medium` | yes | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 73 | `binance_announcements` | Binance Announcements | `crypto` | `global` | 1 | P0 | `crypto_flow_or_crypto_news` | `medium` | yes | Útil para listagens/delistagens/eventos de exchange cripto. |
| 74 | `disclosewt` | Disclose WT | `breaking` | `global` | 2 | P2 | `breaking_news` | `high` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 75 | `bell_geo` | Bell Geopolitics | `geopolitics` | `global` | 2 | P2 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 76 | `geopolitics_live` | Geopolitics Live | `geopolitics` | `global` | 2 | P2 | `geopolitical_or_regional_news` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 77 | `asbmil` | ASB Military News | `military_intel` | `global` | 2 | P1 | `conflict_osint` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 78 | `military_wave` | Military Wave | `conflict` | `global` | 2 | P1 | `conflict_osint` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 79 | `financialjuice` | Financial Juice | `finance` | `global` | 1 | P0 | `finance_market_tape` | `medium` | no | Forte para macro/headline flow; P0 para trade/event-driven. |
| 80 | `zerohedge` | ZeroHedge | `finance` | `global` | 1 | P0 | `finance_market_tape` | `high` | no | Rápido e market-aware; usar como alerta, não como confirmação. |
| 81 | `deita` | Deita | `finance` | `europe` | 2 | P1 | `finance_market_tape` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 82 | `lebanon24` | Lebanon 24 | `conflict` | `middleeast` | 2 | P1 | `conflict_osint` | `medium` | no | Manter em coleta com peso moderado; medir qualidade, duplicação e acurácia histórica antes de subir score. |
| 83 | `intel_sky` | Intel Sky | `military_alert` | `middleeast` | 1 | P0 | `conflict_osint` | `medium` | no | Bom para alertas militares/aviation/Oriente Médio; validar com outras fontes. |
| 84 | `altsignals` | AltSignals (Free) | `forex_signals` | `global` | 3 | P2 | `forex_retail_signal` | `high` | yes | Forex/crypto signal; usar só como retail sentiment/contrarian. |
| 85 | `forexgdp` | Forex GDP (Free) | `forex_signals` | `global` | 3 | P2 | `forex_retail_signal` | `high` | yes | Signal provider; peso baixo até medir histórico. |
| 86 | `unitedsignals` | United Signals (Free) | `forex_signals` | `global` | 3 | P2 | `forex_retail_signal` | `high` | yes | Signal provider; peso baixo até medir histórico. |
| 87 | `prosignalsfx` | ProSignalsFX (Free) | `forex_signals` | `global` | 3 | P2 | `forex_retail_signal` | `high` | yes | Signal provider; peso baixo até medir histórico. |

---

## 10. Validador sugerido para o arquivo

```python
import json
from pathlib import Path
from collections import Counter

REQUIRED_FIELDS = [
    "id",
    "handle",
    "label",
    "sourceGroup",
    "channelSet",
    "topic",
    "region",
    "tier",
    "enabled",
    "maxMessages",
    "publicUrl",
    "captureMethod",
    "decisionUse",
]

def validate(path: str) -> None:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    channels = data.get("channels", [])

    ids = [c.get("id") for c in channels]
    handles = [c.get("handle") for c in channels]

    duplicated_ids = [k for k, v in Counter(ids).items() if v > 1]
    duplicated_handles = [k for k, v in Counter(handles).items() if v > 1]

    if duplicated_ids:
        raise ValueError(f"IDs duplicados: {duplicated_ids}")

    if duplicated_handles:
        print(f"Warning: handles duplicados: {duplicated_handles}")

    for i, c in enumerate(channels):
        missing = [f for f in REQUIRED_FIELDS if f not in c]
        if missing:
            raise ValueError(f"Channel index={i} handle={c.get('handle')} missing={missing}")

        if not str(c["publicUrl"]).startswith("https://t.me/"):
            raise ValueError(f"publicUrl inválida: {c['publicUrl']}")

        if not isinstance(c["decisionUse"], list):
            raise TypeError(f"decisionUse precisa ser lista: {c.get('handle')}")

    data["counts"] = {
        "totalChannelOccurrences": len(channels),
        "enabled": sum(1 for c in channels if c.get("enabled") is True),
        "bySet": dict(Counter(c.get("channelSet", "unknown") for c in channels)),
        "byTopic": dict(Counter(c.get("topic", "unknown") for c in channels)),
        "byTier": dict(Counter(str(c.get("tier", "unknown")) for c in channels)),
        "byRegion": dict(Counter(c.get("region", "unknown") for c in channels)),
    }

    print(json.dumps(data["counts"], indent=2, ensure_ascii=False))

validate("telegram_channels.json")
```

---

## 11. Roadmap recomendado

### Sprint P0

- Corrigir `counts` automaticamente.
- Adicionar `sourceRole`, `biasRisk`, `verificationRequired`, `confidenceClass`.
- Separar topics técnicos:
  - `osint.conflict`
  - `osint.geopolitics`
  - `osint.middleeast`
  - `osint.ukraine`
  - `osint.cyber`
  - `market.finance_tape`
  - `market.crypto_flow`
  - `market.forex_retail_signals`
- Garantir que tudo passa por translator/enricher antes do Oracle.
- Criar scoring mínimo por fonte.

### Sprint P1

- Implementar dedupe por texto normalizado + URL + timestamp bucket.
- Implementar cross-source clustering.
- Calcular reputação histórica por canal.
- Medir ruído por canal.
- Medir latência por canal.
- Criar dashboard de qualidade de fonte.

### Sprint P2

- Validar autenticidade dos canais de marca.
- Rotular viés/família narrativa.
- Integrar com backtest de impacto de headlines.
- Criar circuito de feedback: Oracle marca quais fontes foram úteis para cada decisão.

---

## 12. Conclusão

A lista é forte, mas precisa de pesos e isolamento por tipo de fonte.

A melhor regra é:

```text
Fonte rápida pode disparar alerta.
Fonte rápida não confirma fato sozinha.
Fonte enviesada não é inútil.
Sinal Forex não é OSINT.
Sinal Forex só entra como retail/contrarian.
Oracle só recebe evento enriquecido e com score.
```

Com esses ajustes, o catálogo vira uma base muito boa para inteligência global/event-driven trading.
