import os
import sys
import shutil
import subprocess
import glob
import time
import platform

# Configuration
S3_BUCKET = "s3://elasticmapreduce-uraeusdev/spark_build"
WHEEL_DIR = "dist"
WHEEL_FILE = "streaming_pipeline-0.0.1-py3-none-any.whl"

def run_command(command):
    """Execute a shell command and handle errors."""
    print(f"Executing: {command}")
    try:
        result = subprocess.run(command, check=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"Error output: {e.stderr}")
        sys.exit(1)

def force_remove_windows(directory):
    """Use Windows commands to forcefully remove a directory."""
    try:
        # Using rd /s /q which is more aggressive than rmdir
        cmd = f'rd /s /q "{directory}"'
        subprocess.run(cmd, shell=True, check=True, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False

def clean_directory(directory):
    """Remove a directory if it exists with Windows-specific handling."""
    if not os.path.exists(directory):
        print(f"Directory does not exist, skipping: {directory}")
        return

    print(f"Removing directory: {directory}")
    
    try:
        # First try using shutil
        shutil.rmtree(directory)
        print(f"Successfully removed {directory} using shutil.rmtree")
        return
    except Exception as e:
        print(f"shutil.rmtree failed: {e}")

    # If shutil fails and we're on Windows, try using rd command
    if platform.system() == 'Windows':
        print("Attempting forced removal using Windows rd command...")
        if force_remove_windows(directory):
            print(f"Successfully removed {directory} using rd command")
            return
        
        print(f"Warning: Could not remove {directory}")
        print("Please try manually deleting the directory and run the script again")
        sys.exit(1)
    else:
        # On non-Windows systems, just raise the original error
        raise

def main():
    print("Starting build process...")

    # Clean up old build files
    print("\nCleaning up old build files...")
    for dir_to_remove in ['build', 'dist']:
        clean_directory(dir_to_remove)

    # Remove *.egg-info directories
    for egg_info_dir in glob.glob('*.egg-info') + glob.glob('src/*.egg-info'):
        clean_directory(egg_info_dir)

    # Run setup.py to create the wheel file
    print("\nRunning setup.py to create wheel file...")
    run_command("python setup.py sdist bdist_wheel")

    # Check if the wheel file was created
    wheel_path = os.path.join(WHEEL_DIR, WHEEL_FILE)
    if not os.path.exists(wheel_path):
        print(f"Error: Wheel file not created at {wheel_path}. Build failed.")
        sys.exit(1)
    else:
        print(f"Wheel file created successfully: {wheel_path}")

    # Upload files to S3
    print("\nUploading files to S3...")
    files_to_upload = [
        (wheel_path, f"{S3_BUCKET}/{WHEEL_FILE}"),
        ("spark_runner.py", f"{S3_BUCKET}/spark_runner.py"),
        ("configs/jaas.conf", f"{S3_BUCKET}/jaas.conf"),
        ("configs/LSTM_model.h5", f"{S3_BUCKET}/LSTM_model.h5")
    ]

    for local_file, s3_path in files_to_upload:
        if os.path.exists(local_file):
            run_command(f"aws s3 cp {local_file} {s3_path}")
        else:
            print(f"Warning: File {local_file} does not exist. Skipping upload.")

    print("\nBuild and upload process completed successfully!")

if __name__ == "__main__":
    main()