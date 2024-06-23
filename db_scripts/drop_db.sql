CREATE PROCEDURE DROP_DB()
BEGIN


/*******************************************************
DELETE FROM ALL TABLES
*******************************************************
DELETE FROM PICKS;
DELETE FROM USERS;
DELETE FROM GAMES;
DELETE FROM TEAMS;
DELETE FROM LOCATIONS; */


/*******************************************************
DROP ALL FOREIGN KEY CONSTRAINTS
*******************************************************/
ALTER TABLE GAMES
	DROP FOREIGN KEY FK_GameAwayTeam;
ALTER TABLE GAMES
	DROP CONSTRAINT FK_GameHomeTeam;
ALTER TABLE GAMES
	DROP CONSTRAINT FK_GameLocations;


/*******************************************************
DROP ALL PRIMARY KEY CONSTRAINTS
*******************************************************/
ALTER TABLE PICKS
	DROP CONSTRAINT PK_PICKS;
ALTER TABLE USERS
	DROP PRIMARY KEY;
ALTER TABLE LOCATIONS
	DROP CONSTRAINT PK_LOCATIONS;
ALTER TABLE TEAMS
	DROP CONSTRAINT PK_TEAMS;
ALTER TABLE GAMES
	DROP CONSTRAINT PK_GAMES;


/*******************************************************
DROP ALL TABLES
*******************************************************/
DROP TABLE IF EXISTS PICKS;
DROP TABLE IF EXISTS USERS;
DROP TABLE IF EXISTS LOCATIONS;
DROP TABLE IF EXISTS TEAMS;
DROP TABLE IF EXISTS GAMES;


END