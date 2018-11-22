#!/bin/bash
# The following script is used to view logs of docker containers.
name=`basename "$0"`
usage() {
    echo "=============================================================== USAGE ========================================================="
    echo "$name [-c=container_name] [-d] [-f] [-n=1000] [-t=<error|warning|info|all>] [-w]"
    echo "    -c=NAME or --container=NAME  -> To view the log files of container named 'NAME'."
    echo "                                    Note, if no container is provided, then all log files are shown for all containers"
    echo "    -d or --default              -> default resolves to: $0 --type=all --lines=1000."
    echo "    -f or --follow               -> Follow the log file (essentially tail -f)."
    echo "    -n=NUM or --lines=NUM        -> Prints the last 'NUM' lines instead of last 100 lines of the log file."
    echo "    -t=TYPE or --type=TYPE       -> the type of log file to view where 'TYPE' is one of error(e), warning(w), info(i) or all(a)."
    echo "    -w or --watch                -> Use the Linux watch command to avoid cluttering the terminal window with too many logs."
    echo "                                    Note cannot watch all containers."
    echo "Note: Providing both the watch and follow flags will result in the watch flag taking precedence."
    exit 1
}

# valid_container_name checks if $1 is a valid container name
valid_container_name() {
    if [ "$1" == "all" ]; then
        return
    fi
    found=FALSE
    for container_name in $(docker ps --format "{{.Names}}"); do
        if [ "$1" == "$container_name" ]; then
            found=TRUE
            break
        fi
    done

    if [ "$found" == "FALSE" ]; then
        echo "Not a valid container name."
        exit 1
    fi
}

# valid_log_file_type checks if $1 is a supported log level
valid_log_file_type() {
    if [ "$1" == "all" ] || [ "$1" == "a" ]; then
        log_file_name="all"
        return
    fi
    if [ "$1" == "error" ] || [ "$1" == "e" ]; then
        log_file_name='dfc.ERROR'
        return
    fi
    if [ "$1" == "warning" ] || [ "$1" == "w" ]; then
        log_file_name='dfc.WARNING'
        return
    fi
    if [ "$1" == "info" ] || [ "$1" == "i" ]; then
        log_file_name='dfc.INFO'
        return
    fi
    echo "Not a valid log file type"
    exit 1
}

# container_name_to_folder converts a container name to its respective log directory
container_name_to_folder() {
    temp_container_name=$1
    temp_container_name="${temp_container_name:2}"
    temp_container_name="/tmp/dfc/$temp_container_name/log/"
    directories=("${directories[@]}" "$temp_container_name")
}

# get_container_names gets the container names of all dfc containers
# and adds their log directories to $directories
get_container_names() {
    for container_name in $(docker ps --format "{{.Names}}"); do
        if  [[ $container_name == dfc* ]]; then
            if [[ $container_name =~ ^dfc[0-9]*_(proxy|target)_[0-9]* ]]; then
                container_name=${BASH_REMATCH[0]}
            else
                echo Invalid container name format
                exit 1
            fi
            container_name_to_folder $container_name
        fi
    done
}

# file_path_join joins log directory with a log file in that directory and adds 
# the file path to array $files
file_path_join() {
    if [ "$2" == "all" ] ; then
        file_path_join $1 dfc.ERROR
        file_path_join $1 dfc.WARNING
        file_path_join $1 dfc.INFO
    else
        combined="$1$2"
        if [ -f $combined ]; then
            files=("${files[@]}" "$combined")
        fi
    fi
}

numArgs=$#
line_count=100
follow=FALSE
watch=FALSE
DEFAULT=FALSE
container="all"
LOG_TYPE="none"

################# Parse Arguments #################
for i in "$@"
do
case $i in
    -n=*|--lines=*)
        line_count="${i#*=}"
        shift # past argument=value
        ;;

    -c=*|--container=*)
        container="${i#*=}"
        shift # past argument=value
        valid_container_name $container
        ;;

    -f|--follow)
        follow=TRUE
        watch=FALSE
        shift # past argument
        ;;

    -w|--watch)
        watch=TRUE
        follow=FALSE
        shift # past argument
        ;;

    -t=*|--type=*)
        LOG_TYPE="${i#*=}"
        shift # past argument=value
        valid_log_file_type $LOG_TYPE
        ;;

    -d|--default)
        shift # past argument
        line_count=1000
        break
        ;;

    *)
        usage
        ;;
esac
done

# If no arguments are provided, interactively ask for the parameters
if [ "$numArgs" -eq 0 ]; then
    ################# Containers #################
    echo "Enter name of container you would like to see logs for ('all' for all containers):"
    read container
    if [ -z "$container" ]; then
        echo "No container name supplied. Defaulting to all"
        container="all"
    fi
    valid_container_name $container

    ################# Log Type #################
    echo "Enter type of log file you would like to view. Valid options and their short forms: error(e), warning(w), info(i) or all(a)"
    read LOG_TYPE
    if [ -z "$LOG_TYPE" ]; then
        echo "No log file type supplied. Defaulting to error"
        LOG_TYPE="error"
    fi
    valid_log_file_type $LOG_TYPE
    
    ################# Number of Lines #################
    echo "Enter the number of lines you would like to view"
    read line_count
    if ! [[ "$line_count" =~ ^[0-9]+$ ]] ; then
      echo "Error: '$line_count' is not a number"; exit 1
    fi

    if [ ! "$container" == "all" ]; then
        ################# Follow? #################
        read -p "Follow the log file (y/n)?" choice
        case "$choice" in 
            y|Y )
            follow=TRUE
            ;;
            n|N )
            follow=FALSE
            ;;
            * )
            echo "Invalid input, defaulting to no following"
            follow=FALSE
            ;;
        esac

        ################# Watch? #################
        read -p "Watch the log file (y/n)?" choice
        case "$choice" in 
            y|Y )
            watch=TRUE
            follow=FALSE
            ;;
            n|N )
            watch=FALSE
            ;;
            * )
            echo "Invalid input, defaulting to no watching"
            watch=FALSE
            ;;
        esac
    fi
fi

if [ "$LOG_TYPE" == "none" ]; then
    LOG_TYPE="all"
    log_file_name="all"
fi

################# Determine Command #################
commandPrefix="tail -n $line_count"
if [ "$watch" = TRUE ] && [ "$container" != "all" ]; then
    commandPrefix="watch -n 2 $commandPrefix"
elif [ "$follow" = TRUE ] ; then
    commandPrefix="$commandPrefix -f"
fi

declare -a directories=()
if [ "$container" == "all" ]; then
    get_container_names
else
    container_name_to_folder $container
fi


################# Determine Log Files #################
declare -a files=()
for dir in "${directories[@]}"
do
    file_path_join $dir $log_file_name
done

if [ ${#errors[@]} -eq 0 ]; then
    echo No valid log files found. Exiting...
else
    command="$commandPrefix ${files[@]}"
    eval $command
fi