## Run Microsoft SQLServer 2017 inside a docker container

### Usage

```
    $ make all  # Run mssql
    $ make sqlcmd # run a SQL command
    $ make bcp # run BCP command
    $ make clean # clean up and remove your mssql instance
```

### Arguments
- The default docker container name is `mssql`
- Use ARGS= to pass arguments to either `sqlcmd` or `bcp`

```
    $ make sqlcmd ARGS="-Q 'PRINT \"HELLO\"'"
    HELLO
```

### Environments

Copy `default-win2k12-02.env` to `.win2k12-02.env`, add `SA_PASSWORD=`

```
    $ make sqlcmd_run ENVFILE=.win2k12-02.env
    1> USE xcalardb;
    2> GO
    Changed database context to 'xcalardb'.
    1> QUIT
```

This connects you to the settings specified in the `.win2k12-02.env`
