#!/bin/bash
set -e

# Create the project structure
echo "Creating Airflow project structure..."
mkdir -p dags
mkdir -p plugins
mkdir -p logs
mkdir -p config
mkdir -p tests

# Copy existing files
if [ -f airflow.cfg ]; then
  echo "Backing up existing airflow.cfg to config/airflow.cfg.backup"
  cp airflow.cfg config/airflow.cfg.backup
fi

if [ -f webserver_config.py ]; then
  echo "Moving webserver_config.py to config/ directory"
  cp webserver_config.py config/
fi

# Create sample .env file
echo "Creating sample .env file..."
cat > .env.sample << EOL
# Airflow Core Settings
AIRFLOW_HOME=/home/$(whoami)/airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_FOLDER=/home/$(whoami)/airflow/dags

# Database Settings
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow
AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False

# Webserver Settings
AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8089
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8089
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False
AIRFLOW__WEBSERVER__SECRET_KEY=CHANGE_ME_TO_A_COMPLEX_SECRET_KEY

# Email Settings
AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_USER=your.email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your-app-password
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_MAIL_FROM=your.email@gmail.com
EOL

echo "Creating a sample DAG..."
cat > dags/example_dag.py << EOL
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    
    start = EmptyOperator(
        task_id='start',
    )
    
    echo_hello = BashOperator(
        task_id='echo_hello',
        bash_command='echo "Hello, Airflow!"',
    )
    
    end = EmptyOperator(
        task_id='end',
    )
    
    start >> echo_hello >> end
EOL

# Update systemd files with actual username
echo "Updating systemd service files with current username..."
USER=$(whoami)
GROUP=$(id -gn)

for service_file in systemd/airflow-*.service; do
  if [ -f "$service_file" ]; then
    sed -i "s/<USER>/$USER/g" "$service_file"
    sed -i "s/<GROUP>/$GROUP/g" "$service_file"
    echo "Updated $service_file"
  fi
done

# Create webserver service file
cat > systemd/airflow-webserver.service << EOL
[Unit]
Description=Airflow webserver daemon
After=network.target postgresql.service
Wants=postgresql.service

[Service]
User=$(whoami)
Group=$(id -gn)
Type=simple
Environment="PATH=$HOME/airflow/airflow_venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
Environment="AIRFLOW_HOME=$HOME/airflow"
ExecStart=$HOME/airflow/airflow_venv/bin/airflow webserver --port 8089
Restart=on-failure
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOL

echo "Project setup complete!"
echo "Remember to activate your virtual environment with: source airflow_venv/bin/activate"
echo "Then install requirements with: pip install -r requirements.txt"
echo "See README.md for more details." 