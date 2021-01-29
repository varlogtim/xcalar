 #!/bin/bash

LDAPWHOAMI="/usr/bin/ldapwhoami"
LDAPURI=""
PASSWORD_FILE=""
BIND_DN=""
TLS=""

say () {
    echo >&2 "$*"
}

print_help () {
    cat <<EOF
$0 <options>
-h|--help - print this message
-H|--ldapuri - URI of the authenticating LDAP service
-f|--password-file - file containing the password used for authentication
-b|--bind-dn - distinguished name used for LDAP binding
-l|--ldapwhoami - alternate location of ldapwhoami utility
                  (default: /usr/bin/ldapwhoami)
-t|--tls - use TLS during authentication
EOF
}

parse_args () {
    [ $# -eq 0 ] && set -- --help

    while [ $# -gt 0 ]; do
        cmd="$1"
        shift
        case $cmd in
            -h|--help)
                print_help
                exit 1
                ;;
            -H|--ldapuri)
                LDAPURI="$1"
                shift
                ;;
            -f|--password-file)
                PASSWORD_FILE="$1"
                shift
                ;;
            -b|--bind-dn)
                BIND_DN="$1"
                shift
                ;;
            -l|--ldapwhoami)
                LDAPWHOAMI="$1"
                shift
                ;;
            -t|--tls)
                TLS="-ZZ"
                ;;
            -*)
                echo >&2 "ERROR: Unknown argument $cmd";
                exit 2
                ;;
            *)
                break
                ;;
        esac
    done
}

parse_args "$@"

if [ -z "$LDAPURI" ] || [ -z "$PASSWORD_FILE" ] || \
    [ -z "$BIND_DN" ]; then
    say "One or more of the following mandatory options"
    say "were not used:"
    say "-u|--username"
    say "-H|--ldapuri"
    say "-f|--password-file"
    say "-b|--bind-dn"
    say "Please specify the missing options and try again"
    exit 2
fi

if [ ! -f "$LDAPWHOAMI" ]; then
    say "$LDAPWHOAMI not found"
    exit 3
fi


export LDAPTLS_REQCERT=allow
$LDAPWHOAMI -H "$LDAPURI" -D "$BIND_DN" -x -y "$PASSWORD_FILE" $TLS
rc=$?

exit $rc
