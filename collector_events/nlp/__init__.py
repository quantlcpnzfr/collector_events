# NLP pipeline
#
# Keep this initializer light. The legacy EventProcessor imports
# collector_events.nlp.nlp_engine, which first executes this package file.
# Importing worker/session here creates a circular import through
# collector_events.processors.event_processor.

from .nlp_engine import LocalNLPEngine

__all__ = ["LocalNLPEngine"]

