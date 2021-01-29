
add_custom_target(BUILT_CONFIG
    DEPENDS 90-xcsysctl.conf 90-xclimits.conf)

add_dependencies(installedObjects
    BUILT_CONFIG)

add_custom_command(
    OUTPUT 90-xcsysctl.conf
    COMMAND genSysctl.sh ${CMAKE_CURRENT_SOURCE_DIR}/xcalar-sysctl.json > 90-xcsysctl.conf
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/xcalar-sysctl.json)

add_custom_command(
    OUTPUT 90-xclimits.conf
    COMMAND genLimits.sh ${CMAKE_CURRENT_SOURCE_DIR}/xcalar-limits.json > 90-xclimits.conf
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/xcalar-limits.json)

install(DIRECTORY ld.so.conf.d DESTINATION /etc)
install(FILES xcalar-logrotate.conf RENAME xclogrotate DESTINATION /etc/logrotate.d)
install(FILES default.conf RENAME xcalar DESTINATION /etc/default)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/90-xcsysctl.conf DESTINATION /etc/sysctl.d)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/90-xclimits.conf DESTINATION /etc/security/limits.d)
install(FILES xcalar-rsyslog.conf RENAME 90-xcrsyslog.conf DESTINATION /etc/rsyslog.d)
install(FILES cron.d/xcalar RENAME xcalar DESTINATION /etc/cron.d)
install(FILES xcalar.service DESTINATION /usr/lib/systemd/system)
install(FILES xcalar.slice DESTINATION /usr/lib/systemd/system)
install(FILES caddy.service RENAME xcalar-caddy.service DESTINATION /usr/lib/systemd/system)
install(FILES expserver.service RENAME xcalar-expserver@.service DESTINATION /usr/lib/systemd/system)
install(FILES jupyter.service RENAME xcalar-jupyter.service DESTINATION /usr/lib/systemd/system)
install(FILES sqldf.service RENAME xcalar-sqldf.service DESTINATION /usr/lib/systemd/system)
install(FILES support-asup.service RENAME xcalar-support-asup.service DESTINATION /usr/lib/systemd/system)
install(FILES usrnode.service RENAME xcalar-usrnode.service DESTINATION /usr/lib/systemd/system)
install(FILES xcmgmtd.service RENAME xcalar-xcmgmtd.service DESTINATION /usr/lib/systemd/system)
install(FILES terminal.service RENAME xcalar-terminal.service DESTINATION /usr/lib/systemd/system)
install(FILES xcalar-all-defaults RENAME xcalar DESTINATION etc/default)
install(FILES xcalar-sudo.conf RENAME xcalar DESTINATION /etc/sudoers.d)

install(FILES hdfs-client.xml hdfs-default.xml supervisor.conf Caddyfile DESTINATION /etc/xcalar)
install(FILES xcalar-tuned.conf RENAME tuned.conf DESTINATION /usr/lib/tuned/xcalar)

