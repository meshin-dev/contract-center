#!/bin/bash

function run_command_based_on_answer() {
    local question="$1"
    local command="$2"

    # Prompt the user for input
    read -p "$question (y/n): " answer

    # Check if the answer is "y" or "yes" (case-insensitive)
    if [[ $answer =~ ^[Yy]$|^yes$ ]]; then
        # Run the command here
        echo "Running the command: $command"
        eval "$command"  # Execute the command
    else
        echo "Skipping the command."
    fi
}

run_command_based_on_answer "Should remove all testnet data?" "docker-compose -f local.yml run --rm django python manage.py flush events.TestnetV4Event"
run_command_based_on_answer "Should build and start all containers?" "docker-compose -f local.yml up -d --build"
run_command_based_on_answer "Should start all containers?" "docker-compose -f local.yml up -d"
run_command_based_on_answer "Should make migrations?" "docker-compose -f local.yml run --rm django python manage.py makemigrations"
run_command_based_on_answer "Should migrate?" "docker-compose -f local.yml run --rm django python manage.py migrate"
run_command_based_on_answer "Should create superuser?" "docker-compose -f local.yml run --rm django python manage.py createsuperuser"
