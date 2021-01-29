#!/bin/bash
set -e

install_dir="$1"
ext_enabled_file="$2"

test ! -e "$ext_enabled_file" && exit 0

gui_ext_dir="${install_dir}/opt/xcalar/xcalar-gui/assets/extensions"
gui_ext_dir_backup="${install_dir}/.backup/opt/xcalar/xcalar-gui/assets/extensions"

cd "${gui_ext_dir}/ext-enabled"

xd_extensions=$(cat $ext_enabled_file)

for ext in $xd_extensions; do
    test ! -f "../ext-available/${ext}" && \
        echo "Copying ${gui_ext_dir_backup}/ext-available/${ext}" && \
        cp -p "${gui_ext_dir_backup}/ext-available/${ext}" "../ext-available"
    test ! -e "$ext" && \
        echo "Creating symlink ../ext-available/${ext}" && \
        ln -s "../ext-available/${ext}"
done

ls -l "${gui_ext_dir}/ext-enabled"

exit 0
