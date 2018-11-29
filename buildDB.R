library(RSQLite)
source("dbTools.R")
source("C:/Users/sam/Documents/R/projects/rGrassTools/grassTools.r")

leakyDB=dbConnect(SQLite(),"C:/Users/sam/Documents/LeakyRivers/Data/sqLiteDatabase/LeakyDB.db")
shapePath="C:/Users/sam/Documents/LeakyRivers/Data/sqLiteDatabase/shapes/"
targetEPSG=32613

dbListTables(leakyDB)

#remove all data:
lapply(dbListTables(leakyDB),FUN=delete_data,db=leakyDB)
#run freshStart.sql in r project directory to fully trash and recreate db

############------------define watersheds----------###########
wshedDefs=addWatershedDefinitions( read.csv('C:/Users/sam/Documents/spatial/data/WatershedOutflowPoints/allWsheds_13n.csv') )


###################-------------------add widths (from Mike and I) --------################
widths=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/width/FinalWidths_11_2018.csv")
#can't handle data without coordinates - bother mike about this if necessary
widths=widths[complete.cases(widths[,c("X","Y")]),]
addData( inEPSG=4326)