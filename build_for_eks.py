#!/usr/bin/env python3

## Specify different tag or region
## python build_k8s.py --account-id 703671895821 --tag latest --region us-west-2

import os
import sys
import shutil
import subprocess
import glob
import platform
import argparse

# Configuration
IMAGE_NAME = "fleetconnect-dataplatform"
WHEEL_DIR = "dist"
WHEEL_FILE = "streaming_pipeline-0.0.1-py3-none-any.whl"
DEFAULT_TAG = "spark-k8s"
DEFAULT_REGION = "eu-central-1"

def run_command(command, show_progress=False):
    """Execute a shell command and handle errors."""
    print(f"Executing: {command}")
    try:
        if show_progress:
            # Use Popen and set stdout/stderr to be displayed directly to terminal
            process = subprocess.Popen(
                command,
                shell=True,
                bufsize=0,  # No buffering
                stdout=None,  # Use parent's stdout
                stderr=None   # Use parent's stderr
            )
            
            # Wait for process to complete and check return code
            return_code = process.wait()
            if return_code != 0:
                raise subprocess.CalledProcessError(return_code, command)
            
            return ""
        else:
            # Use regular subprocess.run for non-progress commands
            result = subprocess.run(command, check=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            print(result.stdout)
            return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"Error output: {e.stderr if hasattr(e, 'stderr') else 'No error output available'}")
        sys.exit(1)

def force_remove_windows(directory):
    """Use Windows commands to forcefully remove a directory."""
    try:
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
        shutil.rmtree(directory)
        print(f"Successfully removed {directory} using shutil.rmtree")
        return
    except Exception as e:
        print(f"shutil.rmtree failed: {e}")

    if platform.system() == 'Windows':
        print("Attempting forced removal using Windows rd command...")
        if force_remove_windows(directory):
            print(f"Successfully removed {directory} using rd command")
            return
        
        print(f"Warning: Could not remove {directory}")
        print("Please try manually deleting the directory and run the script again")
        sys.exit(1)
    else:
        raise

def main():
    parser = argparse.ArgumentParser(description="Build and deploy Spark application for Kubernetes")
    parser.add_argument("--tag", default=DEFAULT_TAG, help="Docker image tag")
    parser.add_argument("--region", default=DEFAULT_REGION, help="AWS region")
    parser.add_argument("--account-id", required=True, help="AWS account ID")
    parser.add_argument("--add-wheel", action="store_true", help="Build wheel package before creating image")
    parser.add_argument("--push-only", action="store_true", help="Only push existing image to ECR")
    
    args = parser.parse_args()
    
    # Configuration from arguments
    AWS_REGION = args.region
    AWS_ACCOUNT_ID = args.account_id
    TAG = args.tag
    
    if not args.push_only:
        if args.add_wheel:
            print("Starting wheel build process...")

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
        else:
            print("Skipping wheel build (use --add-wheel to build wheel package)")
        
        # Verify Dockerfile exists
        if not os.path.exists("Dockerfile"):
            print("Error: Dockerfile not found in the current directory.")
            sys.exit(1)
        
        # Build Docker image
        print("\nBuilding Docker image...")
        image_uri = f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com/{IMAGE_NAME}:{TAG}"
        run_command(f"docker build -t {image_uri} .")
    
    # Push to ECR
    print("\nPushing image to ECR...")
    
    # Login to ECR
    run_command(f"aws ecr get-login-password --region {AWS_REGION} | docker login --username AWS --password-stdin {AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com")
    
    # Create repository if it doesn't exist
    try:
        run_command(f"aws ecr describe-repositories --repository-names {IMAGE_NAME} --region {AWS_REGION}")
    except subprocess.CalledProcessError:
        print(f"Repository {IMAGE_NAME} does not exist, creating...")
        run_command(f"aws ecr create-repository --repository-name {IMAGE_NAME} --region {AWS_REGION}")
    
    # Push image to ECR
    image_uri = f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com/{IMAGE_NAME}:{TAG}"
    run_command(f"docker push {image_uri}", show_progress=True)
    
    print(f"\nBuild and deployment completed successfully!")
    print(f"Image URI: {image_uri}")

if __name__ == "__main__":
    main()