#!/bin/bash

ext_enabled_file=$1
xlrdir=$2
user=$3

new_ext_enabled_file="${ext_enabled_file}.new"
diff_ext_enabled_file="${ext_enabled_file}.diff"
pip_log_file="/tmp/$user/pip-install.log"

${xlrdir}/bin/python3.6 -m pip freeze > "$new_ext_enabled_file"

old_requirements=($(comm -23 "$ext_enabled_file" "$new_ext_enabled_file"))

run_pip="0"
rm -f "$diff_ext_enabled_file"
for req in ${old_requirements[*]}; do
    req_name=${req%%=*}

    new_req_name=$(grep "$req_name" "$new_ext_enabled_file")
    req_rc=$?

    [ "$req_rc" == "0" ] && new_req_name=${new_req_name%%=*} && [ "$new_req_name" != "$req_name" ] && echo "$req" >> "$diff_ext_enabled_file" && run_pip="1"
    [ "$req_rc" != "0" ] && echo "$req" >> "$diff_ext_enabled_file" && run_pip="1"
done

if [ "$run_pip" == "1" ]; then
    ${xlrdir}/bin/python3.6 -m pip install -r "$diff_ext_enabled_file" >"$pip_log_file" 2>&1
fi
