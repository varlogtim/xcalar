The main apps are in listSchedule, createSchedule, deleteSchedule, and the
functions named "main" in those files are getting called via appRun.

The listSchedule and deleteSchedule both take a scheduleKey string.
A sample createSchedule input object is contained in sampleCreateObj.txt

executeScheduledRetina is scheduled in cron and then is executed
by cron at a later date.  Its parameters are stored in kvstore,
which is retrieved on execution.  KVstore also serves as a backup
in case of corruption or deletion of the crontab
