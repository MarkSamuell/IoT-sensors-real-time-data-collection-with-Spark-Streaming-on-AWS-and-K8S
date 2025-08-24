import h5py
import logging
import os
import zipfile
import json
from io import BytesIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def inspect_model_file(model_path):
    """Inspect model file supporting both HDF5 (.h5) and ZIP-based (.keras) formats"""
    logger.info(f"\n{'='*50}\nINSPECTING MODEL FILE: {model_path}\n{'='*50}")
    
    if not os.path.exists(model_path):
        logger.error(f"File does not exist: {model_path}")
        return
    
    # Check file size
    file_size = os.path.getsize(model_path)
    logger.info(f"\nFile Information:")
    logger.info(f"Path: {model_path}")
    logger.info(f"Size: {file_size:,} bytes")
    
    # Read first few bytes to determine file type
    with open(model_path, 'rb') as f:
        header = f.read(4)
    
    # Check if it's a ZIP file (keras format)
    if header.startswith(b'PK\x03\x04'):
        logger.info("\nDetected Keras ZIP format (.keras)")
        inspect_keras_zip(model_path)
    else:
        logger.info("\nAttempting to read as HDF5 format (.h5)")
        inspect_hdf5(model_path)

def inspect_keras_zip(model_path):
    """Inspect .keras format (ZIP-based)"""
    try:
        with zipfile.ZipFile(model_path, 'r') as zf:
            # List all files in the archive
            logger.info("\nFiles in .keras archive:")
            for file in zf.namelist():
                logger.info(f"  {file}")
            
            # Read metadata.json
            if 'metadata.json' in zf.namelist():
                with zf.open('metadata.json') as f:
                    metadata = json.load(f)
                logger.info("\nMetadata:")
                for key, value in metadata.items():
                    logger.info(f"  {key}: {value}")
            
            # Read config.json
            if 'config.json' in zf.namelist():
                with zf.open('config.json') as f:
                    config = json.load(f)
                logger.info("\nModel Configuration:")
                logger.info("  Model Type: " + config.get('class_name', 'Unknown'))
                
                # Extract layer information
                layers = config.get('config', {}).get('layers', [])
                logger.info("\nLayer Architecture:")
                for layer in layers:
                    layer_class = layer.get('class_name', 'Unknown')
                    layer_config = layer.get('config', {})
                    logger.info(f"\n  Layer: {layer_class}")
                    
                    # Show important layer configs
                    important_configs = ['name', 'units', 'activation', 'return_sequences']
                    for config_name in important_configs:
                        if config_name in layer_config:
                            logger.info(f"    {config_name}: {layer_config[config_name]}")
                    
                    # Show input/output shape if available
                    if 'batch_input_shape' in layer_config:
                        logger.info(f"    input_shape: {layer_config['batch_input_shape']}")
            
            # Read compiler.json if exists
            if 'compiler.json' in zf.namelist():
                with zf.open('compiler.json') as f:
                    compiler = json.load(f)
                logger.info("\nCompiler Configuration:")
                logger.info(f"  {json.dumps(compiler, indent=2)}")
                
    except Exception as e:
        logger.error(f"Error inspecting .keras file: {str(e)}")

def inspect_hdf5(model_path):
    """Inspect HDF5 format (.h5)"""
    try:
        with h5py.File(model_path, 'r') as f:
            logger.info("\nFile Attributes:")
            for key, value in f.attrs.items():
                logger.info(f"  {key}: {value}")
            
            logger.info("\nFile Structure:")
            def print_structure(name, obj):
                indent = '  ' * name.count('/')
                if isinstance(obj, h5py.Dataset):
                    logger.info(f"{indent}Dataset: {name}")
                    logger.info(f"{indent}  Shape: {obj.shape}")
                    logger.info(f"{indent}  Type: {obj.dtype}")
                elif isinstance(obj, h5py.Group):
                    logger.info(f"{indent}Group: {name}")
                    if hasattr(obj, 'attrs'):
                        for key, value in obj.attrs.items():
                            logger.info(f"{indent}  Attr {key}: {value}")
            
            f.visititems(print_structure)
    except Exception as e:
        logger.error(f"Error reading as HDF5: {str(e)}")

if __name__ == "__main__":
    # Use forward slashes for path
    model_file = "C:/Users/mark.girgis/OneDrive - VxLabs GmbH/DataPlatform/uraeus-dataplatform/configs/LSTM_model.h5"
    inspect_model_file(model_file)