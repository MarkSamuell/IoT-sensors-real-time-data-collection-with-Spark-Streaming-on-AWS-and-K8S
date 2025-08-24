"""
ML LSTM Model Package for CAN Message Analysis

This package contains modules for processing and analyzing CAN bus messages
using a pre-trained LSTM model to detect anomalies.

Modules:
    - app: Main application entry point for model inference
    - classifier: Model prediction and classification logic
    - data_processing: Data preprocessing and feature engineering
    - model_handler: Model loading and management
    - batch_analyze: Spark batch processing for large-scale analysis
    - stream_analyze: Spark streaming analysis for real-time processing

Usage:
    from streaming_pipeline.jobs.ml_lstm_model import batch_analyze, stream_analyze
    batch_analyze.analyze(spark)  # For batch processing
    stream_analyze.analyze(spark)  # For streaming processing
"""

from streaming_pipeline.jobs.ml_lstm_model.app import classify_incoming_data
from streaming_pipeline.jobs.ml_lstm_model.classifier import classify_can_frames
from streaming_pipeline.jobs.ml_lstm_model.data_processing import load_and_process_data
from streaming_pipeline.jobs.ml_lstm_model.model_handler import load_trained_model
from streaming_pipeline.jobs.ml_lstm_model.batch_analyze import analyze as batch_analyze
from streaming_pipeline.jobs.ml_lstm_model.stream_analyze import analyze as stream_analyze

__all__ = [
    'classify_incoming_data',
    'classify_can_frames',
    'load_and_process_data',
    'load_trained_model',
    'batch_analyze',
    'stream_analyze'
]