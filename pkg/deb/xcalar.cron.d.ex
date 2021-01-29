#
# Regular cron jobs for the xcalar package
#
0 4	* * *	root	[ -x /usr/bin/xcalar_maintenance ] && /opt/xcalar/sbin/xcalar_maintenance
