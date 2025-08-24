import pandas as pd
import logging
from streaming_pipeline.jobs.ml_lstm_model.data_processing import load_and_process_data
from streaming_pipeline.jobs.ml_lstm_model.model_handler import load_trained_model
from streaming_pipeline.jobs.ml_lstm_model.classifier import classify_can_frames

logging.basicConfig(level=logging.INFO)

def classify_incoming_data(file_path=None, data_df=None):
    # Step 1: Load and preprocess the dataset
    processed_data = load_and_process_data(file_path=file_path, df=data_df)

    # Step 2: Load the trained model
    model = load_trained_model()

    # Step 3: Perform classification
    predictions = classify_can_frames(model, processed_data)

    # Check if predictions are empty
    if len(predictions) == 0:
        logging.warning("No predictions made.")
        return pd.DataFrame()

    # Step 4: Return results as DataFrame with Arbitration_ID and prediction
    result_df = pd.DataFrame({
        'Arbitration_ID': processed_data['aid_int'],
        'Classification_Result': predictions
    })
    
    return result_df

if __name__ == "__main__":
    result = classify_incoming_data(file_path=r'data\CAN_model_test\thousand_sample_cloud_test_data.csv')
    print(result)