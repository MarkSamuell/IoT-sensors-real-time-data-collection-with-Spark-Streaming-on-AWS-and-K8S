# EMR Job Manager

## Overview
EMR Job Manager is a Flask-based web application for managing and monitoring EMR (Elastic MapReduce) jobs.

## Features
- List active EMR clusters
- Submit new jobs to specific clusters
- View job status for a cluster
- Stop running job steps

## Prerequisites
- Python 3.8+
- AWS Account with EMR access
- AWS CLI configured with appropriate credentials

## Installation

1. Clone the repository
```bash
git clone <repository_url>
cd emr_job_manager
```

2. Create and activate a virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Configure AWS Credentials
- Option 1: AWS CLI Configuration
```bash
aws configure
```

- Option 2: Environment Variables
```bash
export AWS_ACCESS_KEY_ID='your_access_key'
export AWS_SECRET_ACCESS_KEY='your_secret_key'
export AWS_DEFAULT_REGION='your_region'
```

## Running the Application
```bash
python app.py
```

## Environment Variables
- `SECRET_KEY`: Flask application secret key (optional)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_DEFAULT_REGION`: AWS region (default: eu-central-1)

## Security Considerations
- Use IAM roles with minimal required permissions
- Never hard-code AWS credentials
- Use environment variables or AWS IAM roles

## Troubleshooting
- Ensure AWS CLI is configured correctly
- Check IAM permissions for EMR and Step execution
- Verify network connectivity to EMR clusters

## Contributing
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request