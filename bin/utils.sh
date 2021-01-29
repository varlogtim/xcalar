printInColor()
{
    RED='\e[0;31m'
    GREEN='\e[0;32m'
    YELLOW='\e[1;33m'
    DEFAULTCOLOR='\e[0m'

    color=$1
    text=$2

    if [ $color = "red" ]; then
        echo -e "${RED}${text}${DEFAULTCOLOR}"
    elif [ $color = "yellow" ]; then
        echo -e "${YELLOW}${text}${DEFAULTCOLOR}"
    elif [ $color = "green" ]; then
        echo -e "${GREEN}${text}${DEFAULTCOLOR}"
    else
        echo "$text"
    fi
}

