DROP TABLE IF EXISTS leakyDB.DataTypes;
DROP TABLE IF EXISTS leakyDB.Data;
DROP TABLE IF EXISTS leakyDB.Locations;
DROP TABLE IF EXISTS leakyDB.Watersheds;
DROP TABLE IF EXISTS leakyDB.Areas;
DROP TABLE IF EXISTS leakyDB.Points;
DROP TABLE IF EXISTS leakyDB.Batches;

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
	LocationIDX INTEGER PRIMARY KEY UNIQUE NOT NULL,
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
	fileName TEXT,
	EPSG INTEGER
);

CREATE TABLE Points (
	pointIDX INTEGER PRIMARY KEY UNIQUE NOT NULL,
	X REAL,
	Y REAL,
	EPSG INTEGER,
	onStream INTEGER,
	isSnapped INTEGER
);

CREATE TABLE Batches (
	batchIDX INTEGER PRIMARY KEY UNIQUE NOT NULL,
	batchName TEXT,
	importDateTime TEXT,
	source TEXT
);