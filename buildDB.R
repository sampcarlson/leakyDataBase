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
widths$variable[widths$variable=="Wetted width"]="wettedWidth"
widths$variable[widths$variable=="Bank-full width"]="bankfullWidth"
widths$variable[widths$variable=="DepArea_pct"]="depositionalArea"
#data, containing $X, $Y, $dateTime, $value, $QCStatusOK, $metric, $unit, $method
writeDF=widths[,c("X","Y","Date","value","variable","unit","dataType")]
writeDF=plyr::rename(writeDF,replace=c("Date"="dateTime","variable"="metric","dataType"="method"))

writeDF$QCStatusOK=TRUE
addData_points(writeDF,
               batchName="mikeSamWidths",
               batchSource="C:/Users/Sam/Documents/LeakyRivers/Data/width/FinalWidths_11_2018.csv",
               inEPSG=4326,compareData=F)


###############---------------add pfeiffer thesis data####################---------------------
pfeiffer=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/morph/pfeiffer_thesis_data.csv",stringsAsFactors = F)

pfeiffer$x=sapply(pfeiffer$coordinates,splitOnComma,which='x')
pfeiffer$y=sapply(pfeiffer$coordinates,splitOnComma,which='y')

pfeiffer$woodPerArea=pfeiffer$woodLoad_m3 /(pfeiffer$length_m *pfeiffer$width_m )
pfeiffer$coarseSedPerArea=pfeiffer$coarseSed_m3 /(pfeiffer$length_m *pfeiffer$width_m )
pfeiffer$fineSedPerArea=pfeiffer$fineSed_m3 /(pfeiffer$length_m *pfeiffer$width_m )
pfeiffer$pomPerArea=pfeiffer$pom_m3 /(pfeiffer$length_m *pfeiffer$width_m )
pfeiffer=melt(pfeiffer,id.vars = c("Name","x","y"),measure.vars = c("gradient_deg","confinementRatio","confinementCategory",
                                                      "basalArea_m2.ha","length_m","width_m","woodLoad_m3","coarseSed_m3",
                                                      "fineSed_m3","pom_m3","woodPerArea","coarseSedPerArea",
                                                      "fineSedPerArea","pomPerArea"),
              variable.name = "metric")

name_unit_method_list=list(grad=list(old_name="gradient_deg",new_name="slope",unit="degrees",method="pfeifferFeildObs"),
                           confRat=list(old_name="confinementRatio",new_name="confinememtRatio",unit="m m^-1", method="pfeifferFeildObs"),
                           confCat=list(old_name="confinementCategory",new_name="confinementCategory",unit="categorical",method="pfeifferFeildObs"),
                           basArea=list(old_name="basalArea_m2.ha",new_name="basalArea",unit="m^2 ha^-1",method="pfeifferFeildObs"),
                           length=list(old_name="length_m",new_name="segmentLength",unit="m",method="pfeifferFeildObs"),
                           width=list(old_name="width_m",new_name="bankfullWidth",unit="m",method="pfeifferFeildObs"),
                           woodLod=list(old_name="woodLoad_m3",new_name="woodVol",unit="m^3",method="pfeifferFeildObs"),
                           coarse=list(old_name="coarseSed_m3",new_name="coarseSedVol",unit="m^3",method="pfeifferFeildObs"),
                           fine=list(old_name="fineSed_m3",new_name="fineSedVol",unit="m^3",method="pfeifferFeildObs"),
                           pom=list(old_name="pom_m3",new_name="pomVol",unit="m^3",method="pfeifferFeildObs"),
                           arealWood=list(old_name="woodPerArea",new_name="meanWoodDepth",unit="m",method="pfeifferFeildObs"),
                           arealCoarse=list(old_name="coarseSedPerArea",new_name="meanCoarseDepth",unit="m",method="pfeifferFeildObs"),
                           arealFine=list(old_name="fineSedPerArea",new_name="meanFineDepth",unit="m",method="pfeifferFeildObs"),
                           arealPom=list(old_name="pomPerArea",new_name="meanPomDepth",unit="m",method="pfeifferFeildObs"))

pfeiffer=addUnitMethod(pfeiffer,name_unit_method_list)
pfeiffer$QCStatusOK=TRUE
pfeiffer=plyr::rename(pfeiffer,replace=c(x="X",y="Y"))
pfeiffer$dateTime=as.Date("2016/8/1")
addData_points(pfeiffer,
               batchName = "Pfeiffer Thesis Data",
               batchSource="C:/Users/Sam/Documents/LeakyRivers/Data/morph/pfeiffer_thesis_data.csv",
               inEPSG=4326,
               streamSnapDistCells=50)


############--------------bob resp data---------------######
resp=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/resp/bob_respRates_simpleSites.csv",colClasses="character")

        