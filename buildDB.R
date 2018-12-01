library(RSQLite)
library(tidyverse)
library(plyr)
source("dbTools.R")
source("C:/Users/sam/Documents/R/projects/rGrassTools/grassTools.r")

leakyDB=dbConnect(SQLite(),"C:/Users/sam/Documents/LeakyRivers/Data/sqLiteDatabase/LeakyDB.db")
#set all defaults
defaultFlags=list(inEPSG=32613,
           dbEPSG=32613,
           streamSnapDistCells=20,
           onStream=T,
           addMidpoint=T,
           compareData=T,  #disallow identical data values at the same point?
           dbShapePath="C:/Users/sam/Documents/LeakyRivers/Data/sqLiteDatabase/shapes/",
           dbShapeName="leakyArea_")

dbListTables(leakyDB)

#remove all data:
lapply(dbListTables(leakyDB),FUN=delete_data,db=leakyDB)
#run freshStart.sql in r project directory to fully trash and recreate db

#init grass session for all DB processes:
InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/data/dem/leakyRivers/trim/LeakyRiversDEM_rectTrim_knobFix.tif")

############------------define watersheds----------###########
wshedDefs=addWatershedDefinitions( wshedDefs=read.csv('C:/Users/sam/Documents/spatial/data/WatershedOutflowPoints/allWsheds_13n.csv'),
                                  addMidpoint=F, dbShapeName="watershedArea_")


###################-------------------add widths (from Mike and I) --------################
widths=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/width/FinalWidths_11_2018.csv",stringsAsFactors = F)
#can't handle data without coordinates - bother mike about this if necessary
widths=widths[complete.cases(widths[,c("X","Y")]),]
#data, containing $X, $Y, $dateTime, $value, $QCStatusOK, $metric, $unit, $method
writeDF=widths[,c("X","Y","Date","value","variable","unit","dataType")]
writeDF=plyr::rename(writeDF,replace=c("Date"="dateTime","variable"="metric","dataType"="method"))
writeDF$QCStatusOK=TRUE
addData_points(writeDF,
               batchName="mikeSamWidths",
               batchSource="C:/Users/Sam/Documents/LeakyRivers/Data/width/FinalWidths_11_2018.csv",
               inEPSG=4326,compareData=F)
