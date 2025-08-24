#classifier.py
import numpy as np
import pandas as pd
import logging

def classify_can_frames(model, data):
    """Classify CAN frames as normal or anomaly."""
    if data.empty:
        logging.warning("No data provided for classification.")
        return pd.DataFrame()

    reshaped_data = data.values.reshape(data.shape[0], data.shape[1], 1)
    predictions = model.predict(reshaped_data)
    print(type(predictions))
    # predictions = (predictions.flatten()> 0.5).astype(int)
    predicted_classes = np.argmax(predictions, axis=1)  # Shape: (num_samples,)

    # return predictions
    return predicted_classes
