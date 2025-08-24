from tensorflow.keras.models import load_model
import logging
import os

def load_trained_model():
    """Load the pre-trained LSTM model."""
    model_path = os.path.join(os.path.dirname(__file__), 'LSTM_model.h5')
    try:
        model = load_model(model_path)
        logging.info("Model loaded successfully.")
        return model
    except Exception as e:
        logging.error(f"Error loading model: {e}")
        raise