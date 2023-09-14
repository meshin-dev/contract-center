#!/bin/bash

function handle_interrupt() {
    echo "Interrupt detected. Returning to the main menu..."
    main_menu
}

trap handle_interrupt SIGINT

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

function main_menu() {
    echo "Main Menu:"
    options=("Containers" "Data" "Migrations" "Users" "Shell" "Quit")
    select opt in "${options[@]}"
    do
        case $opt in
            "Containers")
                containers_menu
                break
                ;;
            "Data")
                data_menu
                break
                ;;
            "Migrations")
                migrations_menu
                break
                ;;
            "Users")
                users_menu
                break
                ;;
            "Shell")
                docker-compose run --rm django python manage.py shell
                break
                ;;
            "Quit")
                exit 0
                ;;
            *) echo "Invalid option $REPLY";;
        esac
    done
}

function containers_menu() {
    echo "Containers:"
    options=("Rebuild and start all" "Start all" "Stop all" "Scale workers" "Go back")
    select opt in "${options[@]}"
    do
        case $opt in
            "Rebuild and start all")
                docker-compose up -d --build
                containers_menu
                break
                ;;
            "Start all")
                docker-compose up -d
                containers_menu
                break
                ;;
            "Stop all")
                docker-compose down
                containers_menu
                break
                ;;
            "Scale workers")
                read -p "Enter the number of fetch workers to scale: " workers_fetch
                read -p "Enter the number of process workers to scale: " workers_process
                read -p "Enter the number of live events listener workers to scale: " workers_live
                docker-compose up -d --scale worker_events_fetch=$workers_fetch --scale worker_events_process=$workers_process --scale live_events_listener=$workers_live
                containers_menu
                break
                ;;
            "Go back")
                main_menu
                break
                ;;
            *) echo "Invalid option $REPLY";;
        esac
    done
}

function data_menu() {
    echo "Data:"
    options=("Load fixtures" "Save fixtures" "Erase Testnet Data" "Go back")
    select opt in "${options[@]}"
    do
        case $opt in
            "Load fixtures")
                function load_fixtures() {
                    docker-compose run --rm django python manage.py loaddata interval_schedule.json || exit 1
                    docker-compose run --rm django python manage.py loaddata clocked_schedule.json || exit 1
                    docker-compose run --rm django python manage.py loaddata crontab_schedule.json || exit 1
                    docker-compose run --rm django python manage.py loaddata solar_schedule.json || exit 1
                    docker-compose run --rm django python manage.py loaddata periodic_task.json || exit 1
                    docker-compose run --rm django python manage.py loaddata sync.json
                }
                run_command_based_on_answer "Should load all fixtures?" "load_fixtures"
                data_menu
                break
                ;;
            "Save fixtures")
                function dump_fixtures() {
                    docker-compose run --rm django python manage.py dumpdata django_celery_beat.IntervalSchedule --indent 4 -o contract_center/fixtures/interval_schedule.json || (echo "Failed to dump interval schedule"; exit 1)
                    docker-compose run --rm django python manage.py dumpdata django_celery_beat.ClockedSchedule --indent 4 -o contract_center/fixtures/clocked_schedule.json || (echo "Failed to dump clocked schedule"; exit 1)
                    docker-compose run --rm django python manage.py dumpdata django_celery_beat.CrontabSchedule --indent 4 -o contract_center/fixtures/crontab_schedule.json || (echo "Failed to dump crontab schedule"; exit 1)
                    docker-compose run --rm django python manage.py dumpdata django_celery_beat.SolarSchedule --indent 4 -o contract_center/fixtures/solar_schedule.json || (echo "Failed to dump solar schedule"; exit 1)
                    docker-compose run --rm django python manage.py dumpdata django_celery_beat.PeriodicTask --indent 4 -o contract_center/fixtures/periodic_task.json || (echo "Failed to dump periodic tasks"; exit 1)
                    docker-compose run --rm django python manage.py dumpdata contract.Sync --indent 4 -o contract_center/fixtures/sync.json || (echo "Failed to dump syncs"; exit 1)
                }
                run_command_based_on_answer "Should dump all fixtures?" "dump_fixtures"
                data_menu
                break
                ;;
            "Erase Testnet Data")
                read -p "Data versions: " data_versions
                run_command_based_on_answer "Sure?" "docker-compose run --rm django python manage.py flush --data-versions $data_versions --models contract_center.ssv_network.models.events.TestnetEvent,contract_center.ssv_network.operators.models.operators.TestnetOperator"
                data_menu
                break
                ;;
            "Go back")
                main_menu
                break
                ;;
            *) echo "Invalid option $REPLY";;
        esac
    done
}

function migrations_menu() {
    echo "Migrations:"
    options=("Make migrations" "Migrate" "Go back")
    select opt in "${options[@]}"
    do
        case $opt in
            "Make migrations")
                run_command_based_on_answer "Should make migrations?" "docker-compose run --rm django python manage.py makemigrations"
                migrations_menu
                break
                ;;
            "Migrate")
                run_command_based_on_answer "Should migrate?" "docker-compose run --rm django python manage.py migrate"
                migrations_menu
                break
                ;;
            "Go back")
                main_menu
                break
                ;;
            *) echo "Invalid option $REPLY";;
        esac
    done
}

function users_menu() {
    echo "Users:"
    options=("Create superuser" "Go back")
    select opt in "${options[@]}"
    do
        case $opt in
            "Create superuser")
                docker-compose run --rm django python manage.py createsuperuser
                users_menu
                break
                ;;
            "Go back")
                main_menu
                break
                ;;
            *) echo "Invalid option $REPLY";;
        esac
    done
}

# Run the main menu function
while true; do
    main_menu
done
