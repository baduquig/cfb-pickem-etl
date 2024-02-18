CREATE TABLE PICKEM_GAMES (
    GAME_ID             VARCHAR(15)     NOT NULL,
    LEAGUE              VARCHAR(5)      NOT NULL,
    AWAY_TEAM           VARCHAR(5)      NULL,
    HOME_TEAM           VARCHAR(5)      NULL,
    LOCATION            TINYINT         NULL,
    TV_COVERAGE         VARCHAR(50)     NULL,
    BETTING_LINE        VARCHAR(25)     NULL,
    BETTING_OVER_UNDER  VARCHAR(25)     NULL,
    ATTENDANCE          SMALLINT        NULL,
    AWAY_WIN_PCT        VARCHAR(25)     NULL,
    HOME_WIN_PCT        VARCHAR(25)     NULL,
    AWAY_QUARTER1       TINYINT         NULL,
    AWAY_QUARTER2       TINYINT         NULL,
    AWAY_QUARTER3       TINYINT         NULL,
    AWAY_QUARTER4       TINYINT         NULL,
    AWAY_OVERTIME       TINYINT         NULL,
    AWAY_TOTAL          TINYINT         NULL,
    HOME_QUARTER1       TINYINT         NULL,
    HOME_QUARTER2       TINYINT         NULL,
    HOME_QUARTER3       TINYINT         NULL,
    HOME_QUARTER4       TINYINT         NULL,
    HOME_OVERTIME       TINYINT         NULL,
    HOME_TOTAL          TINYINT         NULL,
    GAME_TIME           VARCHAR(25)     NULL,
    GAME_DATE           VARCHAR(50)     NULL,
    GAME_MONTH          TINYINT         NULL,
    GAME_DAY            TINYINT         NULL,
    GAME_YEAR           TINYINT         NULL
);

CREATE TABLE PICKEM_TEAMS (
    TEAM_ID             VARCHAR(5)      NOT NULL,
    LEAGUE              VARCHAR(5)      NOT NULL,
    NAME                VARCHAR(30)     NOT NULL,
    MASCOT              VARCHAR(30)     NOT NULL,
    LOGO_URL            VARCHAR(75)     NULL,
    CONFERENCE_NAME     VARCHAR(30)     NULL,
    CONFERENCE_WINS     TINYINT         NULL,
    CONFERENCE_LOSSES   TINYINT         NULL,
    CONFERENCE_TIES     TINYINT         NULL,
    OVERALL_WINS        TINYINT         NULL,
    OVERALL_LOSSES      TINYINT         NULL,
    OVERALL_TIES        TINYINT         NULL
);

CREATE TABLE PICKEM_LOCATIONS (
    LEAGUE              VARCHAR(5)      NOT NULL,
    LOCATION_ID         TINYINT         NOT NULL,
    STADIUM             VARCHAR(50)     NULL,
    STADIUM_CAPACITY    MEDIUMINT       NULL,
    CITY                VARCHAR(50)     NULL,
    STATE               VARCHAR(25)     NULL,
    LATITUDE            DECIMAL(18, 15) NULL,
    LONGITUDE           DECIMAL(18, 15) NULL
);