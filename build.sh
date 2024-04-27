#!/bin/bash

# Function to install R based on package manager
install_r_linux() {
    if command -v apt &> /dev/null; then
        sudo apt update
        sudo apt install -y r-base
    elif command -v yum &> /dev/null; then
        sudo yum install -y R
    else
        echo "Neither apt nor yum package manager found. Please install R manually."
        exit 1
    fi
}

# Function to check and install R based on the operating system
install_r() {
    echo "Rscript not found. Do you want to install R? (y/n)"
    read answer

    if [ "$answer" != "${answer#[Yy]}" ]; then
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            install_r_linux
        elif [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            if ! command -v brew &> /dev/null; then
                echo "Homebrew not found. Installing Homebrew..."
                /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
            fi
            brew install R
        elif [[ "$OSTYPE" == "cygwin" || "$OSTYPE" == "msys" ]]; then
            # Windows with a Unix-like environment
            echo "Please install R manually from https://cran.r-project.org/bin/windows/base/ and rerun the script."
            exit 1
        else
            echo "Unknown operating system. Please install R manually."
            exit 1
        fi
    else
        echo "R installation aborted."
        exit 1
    fi
}

# Check if Rscript is installed
if ! command -v Rscript &> /dev/null; then
    install_r
fi

# Execute the R script
Rscript Main.R

# Check the exit status
if [ $? -eq 0 ]; then
    echo "R script executed successfully."
else
    echo "R script encountered an error."
    exit 1
fi
