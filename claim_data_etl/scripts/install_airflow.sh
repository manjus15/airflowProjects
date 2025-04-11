#!/bin/bash
set -e

echo "Airflow Installation and Setup Script"
echo "-------------------------------------"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Installing..."
    sudo apt update
    sudo apt install -y python3 python3-pip python3-venv
else
    echo "Python 3 is already installed: $(python3 --version)"
fi

# Create virtual environment if it doesn't exist
if [ ! -d "airflow_venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv airflow_venv
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

# Activate virtual environment
echo "Activating virtual environment..."
source airflow_venv/bin/activate

# Install or upgrade pip
pip install --upgrade pip

# Install Airflow and dependencies
echo "Installing Apache Airflow and dependencies..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "requirements.txt not found. Installing default Airflow packages..."
    AIRFLOW_VERSION=2.10.4
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    pip install apache-airflow-providers-postgres
    
    # Create requirements.txt file
    pip freeze > requirements.txt
    echo "Created requirements.txt with installed packages."
fi

# Set AIRFLOW_HOME
export AIRFLOW_HOME=$(pwd)
echo "Set AIRFLOW_HOME to: $AIRFLOW_HOME"

# Create project structure
echo "Creating project structure..."
mkdir -p dags plugins logs config tests

# Initialize Airflow database if not already done
if [ ! -f "airflow.db" ] && [ ! -f "airflow.cfg" ]; then
    echo "Initializing Airflow database..."
    airflow db init
    echo "Airflow database initialized."
else
    echo "Airflow already initialized. Checking for database upgrades..."
    airflow db check
fi

# Create admin user if not exists
if ! airflow users list | grep -q "admin"; then
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    echo "Admin user created."
else
    echo "Admin user already exists."
fi

# Create systemd service files if they don't exist
if [ ! -d "systemd" ]; then
    echo "Creating systemd service files..."
    mkdir -p systemd
    
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
    
    # Create scheduler service file
    cat > systemd/airflow-scheduler.service << EOL
[Unit]
Description=Airflow scheduler daemon
After=network.target postgresql.service
Wants=postgresql.service

[Service]
User=$(whoami)
Group=$(id -gn)
Type=simple
Environment="PATH=$HOME/airflow/airflow_venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
Environment="AIRFLOW_HOME=$HOME/airflow"
ExecStart=$HOME/airflow/airflow_venv/bin/airflow scheduler
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOL
    
    # Create triggerer service file
    cat > systemd/airflow-triggerer.service << EOL
[Unit]
Description=Airflow triggerer daemon
After=network.target postgresql.service
Wants=postgresql.service

[Service]
User=$(whoami)
Group=$(id -gn)
Type=simple
Environment="PATH=$HOME/airflow/airflow_venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
Environment="AIRFLOW_HOME=$HOME/airflow"
ExecStart=$HOME/airflow/airflow_venv/bin/airflow triggerer
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOL
    
    echo "Systemd service files created."
fi

# Create .env.sample file if it doesn't exist
if [ ! -f ".env.sample" ]; then
    echo "Creating sample .env file..."
    cat > .env.sample << EOL
# Airflow Core Settings
AIRFLOW_HOME=$(pwd)
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags

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
    echo "Sample .env file created."
fi

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "PostgreSQL is not installed. For production, it's recommended to install PostgreSQL."
    echo "You can install PostgreSQL with: sudo apt install postgresql postgresql-contrib"
    echo "Then follow the guide in docs/postgres_setup.md"
else
    echo "PostgreSQL is already installed: $(psql --version)"
fi

echo ""
echo "Airflow Installation and Setup Complete!"
echo ""
echo "To start Airflow services manually:"
echo "1. In one terminal: airflow webserver --port 8080"
echo "2. In another terminal: airflow scheduler"
echo ""
echo "To install systemd services (for automatic startup):"
echo "1. Edit the files in the systemd/ directory if needed"
echo "2. Run: sudo cp systemd/airflow-*.service /etc/systemd/system/"
echo "3. Run: sudo systemctl daemon-reload"
echo "4. Run: sudo systemctl enable airflow-webserver airflow-scheduler"
echo "5. Run: sudo systemctl start airflow-webserver airflow-scheduler"
echo ""
echo "For more information, see the README.md and documentation in the docs/ directory." 