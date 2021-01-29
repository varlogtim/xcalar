PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS perfTest(
    testid      INTEGER PRIMARY KEY NOT NULL,
    testname    TEXT                NOT NULL,
    testhash    TEXT                NOT NULL
);

CREATE TABLE IF NOT EXISTS suiteRun(
    suiteid     INTEGER PRIMARY KEY NOT NULL,
    timestamp   INTEGER             NOT NULL,
    gitsha      TEXT
);

CREATE TABLE IF NOT EXISTS testRun(
    runid       INTEGER PRIMARY KEY NOT NULL,
    testid      INTEGER             NOT NULL,
    suiteid     INTEGER             NOT NULL,

    FOREIGN KEY(testid) REFERENCES perfTest(testid),
    FOREIGN KEY(suiteid) REFERENCES suiteRun(suiteid)
);

CREATE TABLE IF NOT EXISTS commands(
    commandid   INTEGER PRIMARY KEY NOT NULL,
    runid       INTEGER             NOT NULL,
    stepnum     INTEGER             NOT NULL,
    cmdstring   TEXT                NOT NULL,
    secTaken    REAL                NOT NULL,

    FOREIGN KEY(runid) REFERENCES testRun(runid)
);
