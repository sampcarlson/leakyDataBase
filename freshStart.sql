DROP TABLE IF EXISTS leakyDB.DataTypes;
DROP TABLE IF EXISTS leakyDB.Data;
DROP TABLE IF EXISTS leakyDB.Locations;
DROP TABLE IF EXISTS leakyDB.Watersheds;
DROP TABLE IF EXISTS leakyDB.Areas;
DROP TABLE IF EXISTS leakyDB.Points;
DROP TABLE IF EXISTS leakyDB.Batches;
DROP TABLE IF EXISTS leakyDB.PointsInAreas;

CREATE TABLE DataTypes (
    dataTypeIDX INTEGER PRIMARY KEY UNIQUE NOT NULL,
	metric TEXT NOT NULL,
	unit TEXT NOT NULL,
	method TEXT NOT NULL	
);

CREATE TABLE Data (
	dataIDX INTEGER PRIMARY KEY UNIQUE NOT NULL,
	dataTypeIDX INTEGER NOT NULL,
	batchIDX INTEGER,
	locationIDX INTEGER NOT NULL,
	dateTime TEXT,
	value  TEXT NOT NULL,
	QCStatusOK INTEGER
);

CREATE TABLE Locations (
	locationIDX INTEGER PRIMARY KEY UNIQUE NOT NULL,
	isPoint INTEGER,
	pointIDX INTEGER,
	areaIDX INTEGER,
	watershedID TEXT
);

CREATE TABLE Watersheds (
	watershedID TEXT PRIMARY KEY UNIQUE NOT NULL,
	areaIDX INTEGER,
	outPointIDX INTEGER
);

CREATE TABLE Areas (
	areaIDX INTEGER PRIMARY KEY UNIQUE NOT NULL,
	name TEXT,
	fileName TEXT,
	EPSG INTEGER
);

CREATE TABLE Points (
	pointIDX INTEGER PRIMARY KEY UNIQUE NOT NULL,
	X REAL(10),
	Y REAL(10),
	EPSG INTEGER,
	onStream INTEGER
);

CREATE TABLE Batches (
	batchIDX INTEGER PRIMARY KEY UNIQUE NOT NULL,
	batchName TEXT,
	importDateTime TEXT,
	source TEXT
);

CREATE TABLE PointsInAreas (
	pointIDX INTEGER,
	areaIDX INTEGER
);