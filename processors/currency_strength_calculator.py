# services/collector_events/app/processors/currency_strength_calculator.py

from datetime import datetime, timedelta
from typing import Dict, List
import numpy as np
from pymongo import MongoClient
from forex_shared.database import get_db
from forex_shared.models import CurrencyStrength, EventImpact

class CurrencyStrengthCalculator:
    """
    Calcula strength score de cada moeda baseado em eventos
    """
    
    CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'NZD']
    
    def __init__(self, mongo_client: MongoClient):
        self.mongo = mongo_client
        self.news_collection = self.mongo.forex_ai.news_events
    
    def calculate_all_currencies(self):
        """Calcula strength de todas as moedas"""
        
        results = {}
        
        for currency in self.CURRENCIES:
            strength_data = self.calculate_currency_strength(currency)
            results[currency] = strength_data
            
            # Salvar no PostgreSQL
            self._save_currency_strength(currency, strength_data)
        
        return results
    
    def calculate_currency_strength(self, currency: str) -> Dict:
        """
        Calcula strength score de uma moeda
        
        Baseado em:
        1. Política Monetária (40%)
        2. Dados Econômicos (30%)
        3. Geopolítica (20%)
        4. Sentiment de Mercado (10%)
        """
        
        # Buscar eventos recentes (últimas 48 horas)
        cutoff_time = datetime.utcnow() - timedelta(hours=48)
        
        events = list(self.news_collection.find({
            'published_at': {'$gte': cutoff_time},
            'processed': True,
            f'nlp_analysis.predicted_impact.{currency}': {'$exists': True}
        }).sort('published_at', -1))
        
        if not events:
            return self._default_strength()
        
        # Calcular componentes
        monetary_score = self._calculate_monetary_policy_score(events, currency)
        economic_score = self._calculate_economic_data_score(events, currency)
        geopolitical_score = self._calculate_geopolitical_score(events, currency)
        sentiment_score = self._calculate_market_sentiment_score(events, currency)
        
        # Score final (média ponderada)
        weights = {
            'monetary': 0.40,
            'economic': 0.30,
            'geopolitical': 0.20,
            'sentiment': 0.10
        }
        
        strength_score = (
            monetary_score * weights['monetary'] +
            economic_score * weights['economic'] +
            geopolitical_score * weights['geopolitical'] +
            sentiment_score * weights['sentiment']
        )
        
        # Calcular momentum (taxa de mudança)
        momentum = self._calculate_momentum(currency)
        
        # Determinar trend
        trend = self._determine_trend(strength_score, momentum)
        
        return {
            'currency': currency,
            'strength_score': strength_score,
            'momentum': momentum,
            'trend': trend,
            'components': {
                'monetary_policy': monetary_score,
                'economic_data': economic_score,
                'geopolitical': geopolitical_score,
                'market_sentiment': sentiment_score
            },
            'recent_events': [e['event_id'] for e in events[:10]],
            'calculated_at': datetime.utcnow(),
            'confidence': self._calculate_confidence(events)
        }
    
    def _calculate_monetary_policy_score(self, events: List, currency: str) -> float:
        """
        Score de política monetária (-100 a +100)
        
        Hawkish = positivo para moeda (score alto)
        Dovish = negativo para moeda (score baixo)
        """
        
        monetary_events = [
            e for e in events 
            if e.get('category') == 'MONETARY_POLICY'
            and currency in e.get('nlp_analysis', {}).get('predicted_impact', {})
        ]
        
        if not monetary_events:
            return 0.0
        
        scores = []
        
        for event in monetary_events:
            stance = event['nlp_analysis'].get('stance', 'neutral')
            impact = event['nlp_analysis']['predicted_impact'][currency]
            
            # Hawkish = +score, Dovish = -score
            if stance == 'hawkish':
                base_score = 50
            elif stance == 'dovish':
                base_score = -50
            else:
                base_score = 0
            
            # Ajustar por magnitude
            magnitude = impact.get('magnitude', 0.5)
            event_score = base_score * magnitude
            
            # Decay por tempo (eventos mais recentes pesam mais)
            hours_ago = (datetime.utcnow() - event['published_at']).total_seconds() / 3600
            time_weight = np.exp(-hours_ago / 24)  # Decay exponencial
            
            scores.append(event_score * time_weight)
        
        return np.mean(scores) if scores else 0.0
    
    def _calculate_economic_data_score(self, events: List, currency: str) -> float:
        """
        Score de dados econômicos (-100 a +100)
        
        Dados melhores que esperado = positivo
        Dados piores que esperado = negativo
        """
        
        economic_events = [
            e for e in events 
            if e.get('category') == 'ECONOMIC_DATA'
            and currency in e.get('nlp_analysis', {}).get('predicted_impact', {})
        ]
        
        if not economic_events:
            return 0.0
        
        scores = []
        
        for event in economic_events:
            impact = event['nlp_analysis']['predicted_impact'][currency]
            direction = impact.get('direction', 'NEUTRAL')
            magnitude = impact.get('magnitude', 0.5)
            
            if direction == 'UP':
                event_score = 40 * magnitude
            elif direction == 'DOWN':
                event_score = -40 * magnitude
            else:
                event_score = 0
            
            # Time decay
            hours_ago = (datetime.utcnow() - event['published_at']).total_seconds() / 3600
            time_weight = np.exp(-hours_ago / 12)
            
            scores.append(event_score * time_weight)
        
        return np.mean(scores) if scores else 0.0
    
    def _calculate_geopolitical_score(self, events: List, currency: str) -> float:
        """Score de eventos geopolíticos"""
        
        geo_events = [
            e for e in events 
            if e.get('category') == 'GEOPOLITICAL'
            and currency in e.get('nlp_analysis', {}).get('predicted_impact', {})
        ]
        
        if not geo_events:
            return 0.0
        
        scores = []
        
        for event in geo_events:
            sentiment = event['nlp_analysis'].get('sentiment', 0)
            impact = event['nlp_analysis']['predicted_impact'][currency]
            magnitude = impact.get('magnitude', 0.5)
            
            # Eventos negativos (guerra) = bad for most currencies
            # Moedas safe-haven (USD, JPY, CHF) podem se beneficiar
            safe_havens = ['USD', 'JPY', 'CHF']
            
            if currency in safe_havens and sentiment < -0.5:
                event_score = 30 * magnitude  # Beneficiado por crise
            else:
                event_score = sentiment * 30 * magnitude
            
            hours_ago = (datetime.utcnow() - event['published_at']).total_seconds() / 3600
            time_weight = np.exp(-hours_ago / 48)  # Eventos geo duram mais
            
            scores.append(event_score * time_weight)
        
        return np.mean(scores) if scores else 0.0
    
    def _calculate_market_sentiment_score(self, events: List, currency: str) -> float:
        """Score agregado de sentiment"""
        
        sentiments = []
        
        for event in events:
            if currency in event.get('nlp_analysis', {}).get('predicted_impact', {}):
                sentiment = event['nlp_analysis'].get('sentiment', 0)
                
                hours_ago = (datetime.utcnow() - event['published_at']).total_seconds() / 3600
                time_weight = np.exp(-hours_ago / 6)  # Sentiment muda rápido
                
                sentiments.append(sentiment * 20 * time_weight)
        
        return np.mean(sentiments) if sentiments else 0.0
    
    def _calculate_momentum(self, currency: str) -> float:
        """
        Calcula taxa de mudança do strength score
        
        Compara score atual com 24h atrás
        """
        
        db = next(get_db())
        
        current = db.query(CurrencyStrength).filter(
            CurrencyStrength.currency == currency
        ).order_by(CurrencyStrength.calculated_at.desc()).first()
        
        past = db.query(CurrencyStrength).filter(
            CurrencyStrength.currency == currency,
            CurrencyStrength.calculated_at < datetime.utcnow() - timedelta(hours=24)
        ).order_by(CurrencyStrength.calculated_at.desc()).first()
        
        if not current or not past:
            return 0.0
        
        return current.strength_score - past.strength_score
    
    def _determine_trend(self, score: float, momentum: float) -> str:
        """Determina trend da moeda"""
        
        if momentum > 10:
            return 'STRENGTHENING'
        elif momentum < -10:
            return 'WEAKENING'
        else:
            return 'STABLE'
    
    def _calculate_confidence(self, events: List) -> float:
        """
        Confiança no score calculado
        
        Baseado em:
        - Número de eventos
        - Consistência dos sinais
        """
        
        if len(events) < 2:
            return 0.3
        elif len(events) < 5:
            return 0.6
        else:
            return 0.9
    
    def _default_strength(self) -> Dict:
        """Retorna dados padrão quando não há eventos"""
        
        return {
            'strength_score': 0.0,
            'momentum': 0.0,
            'trend': 'STABLE',
            'components': {
                'monetary_policy': 0.0,
                'economic_data': 0.0,
                'geopolitical': 0.0,
                'market_sentiment': 0.0
            },
            'recent_events': [],
            'calculated_at': datetime.utcnow(),
            'confidence': 0.0
        }
    
    def _save_currency_strength(self, currency: str, strength_data: Dict):
        """Salva no PostgreSQL"""
        
        db = next(get_db())
        
        strength = CurrencyStrength(
            currency=currency,
            strength_score=strength_data['strength_score'],
            momentum=strength_data['momentum'],
            monetary_policy_score=strength_data['components']['monetary_policy'],
            economic_data_score=strength_data['components']['economic_data'],
            geopolitical_score=strength_data['components']['geopolitical'],
            market_sentiment_score=strength_data['components']['market_sentiment'],
            recent_events=strength_data['recent_events'],
            calculated_at=strength_data['calculated_at'],
            trend=strength_data['trend'],
            confidence=strength_data['confidence']
        )
        
        db.add(strength)
        db.commit()