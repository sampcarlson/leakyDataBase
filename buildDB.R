library(RSQLite)
library(tidyverse)
library(plyr)
library(reshape2)
library(rgrass7)
#source("dbTools.R")
source("C:/Users/sam/Documents/R/projects/rGrassTools/grassTools.r")
source('~/R/projects/leakyDataBase/dbTools.R')
source('~/R/projects/leakyDataBase/BuildHugeStreamNetwork.R')
source('~/R/projects/leakyDataBase/widthWrangler.R')
source('~/R/projects/leakyDataBase/createStreamSegsDF.R')
source('~/R/projects/leakyDataBase/widthWrangler_byTransect.R')


leakyDB=dbConnect(SQLite(),"C:/Users/sam/Documents/LeakyRivers/Data/sqLiteDatabase/LeakyDB.db")


#set all defaults
defaultFlags=list(inEPSG=32613,
                  dbEPSG=32613,
                  streamSnapDistCells=20,
                  onStream=F,
                  addMidpoint=T,
                  compareData=T,  #disallow identical data values at the same point?
                  dbShapePath="C:/Users/sam/Documents/LeakyRivers/Data/sqLiteDatabase/shapes/",
                  dbShapeName="leakyArea_")

dbListTables(leakyDB)

#remove all data:
lapply(dbListTables(leakyDB),FUN=delete_data,db=leakyDB)
#run freshStart.sql in r project directory to fully trash and recreate db

################------------define points representing every 100 m reach w/ reach slope data-----------------#############
#go have lunch & a beer or two - this takes a while
#rebuild stream network info - very long process w/ shorter seg lengths

#buildHugeStreamNetwork(segLength=100)
#createStreamSegsDF()

#init grass session for all DB processes, and 
InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/data/dem/leakyRivers/trim/LeakyRiversDEM_rectTrim_knobFix.tif")



############------------define watersheds----------###########
wshedDefs=addWatershedDefinitions( wshedDefs=read.csv('C:/Users/sam/Documents/spatial/data/WatershedOutflowPoints/allWsheds_13n.csv'),
                                   addMidpoint=F, dbShapeName="watershedArea_")


###################-------------------add widths (from Mike and I) --------################
widths=wrangleWidths()
names(widths)[names(widths)=="x"]="X"
names(widths)[names(widths)=="y"]="Y"

cc=countChannelsByTransect()
cc$variable="channelCountByTransect"
cc$dataType="observation"
cc$unit="count"
cc$date="2015-09-15"
cc=plyr::rename(cc,replace=c("x"="value"))
cc$Transect=NULL

widths=rbind(widths,cc)

widths$areaName=widths$Site
widths$areaPath=paste0("C:/Users/Sam/Documents/LeakyRivers/Data/width/widthAreasFine/",widths$areaName,".shp")


writeDF=widths[,c("date","value","variable","unit","dataType","areaName","areaPath")]
writeDF=plyr::rename(writeDF,replace=c("date"="dateTime","variable"="metric","dataType"="method"))

writeDF$QCStatusOK=TRUE
addData(writeDF,
        batchName="mikeSamWidths",
        batchSource="widthWrangler.R",
        inEPSG=4326,compareData=F,
        addMidpoint=T,
        streamSnapDistCells=50)


###############---------------add pfeiffer thesis data####################---------------------
# pfeiffer=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/morph/pfeiffer_thesis_data.csv",stringsAsFactors = F)
# 
# pfeiffer$x=sapply(pfeiffer$coordinates,splitOnComma,which='x')
# pfeiffer$y=sapply(pfeiffer$coordinates,splitOnComma,which='y')
# 
# pfeiffer$woodPerArea=pfeiffer$woodLoad_m3 /(pfeiffer$length_m *pfeiffer$width_m )
# pfeiffer$coarseSedPerArea=pfeiffer$coarseSed_m3 /(pfeiffer$length_m *pfeiffer$width_m )
# pfeiffer$fineSedPerArea=pfeiffer$fineSed_m3 /(pfeiffer$length_m *pfeiffer$width_m )
# pfeiffer$pomPerArea=pfeiffer$pom_m3 /(pfeiffer$length_m *pfeiffer$width_m )
# pfeiffer=melt(pfeiffer,id.vars = c("Name","x","y"),measure.vars = c("gradient_deg","confinementRatio","confinementCategory",
#                                                                     "basalArea_m2.ha","length_m","width_m","woodLoad_m3","coarseSed_m3",
#                                                                     "fineSed_m3","pom_m3","woodPerArea","coarseSedPerArea",
#                                                                     "fineSedPerArea","pomPerArea"),
#               variable.name = "metric")
# 
# name_unit_method_list=list(grad=list(old_name="gradient_deg",new_name="slope",unit="degrees",method="Pfeiffer survey"),
#                            confRat=list(old_name="confinementRatio",new_name="confinememtRatio",unit="m m^-1", method="Pfeiffer survey"),
#                            confCat=list(old_name="confinementCategory",new_name="confinementCategory",unit="categorical",method="Pfeiffer survey"),
#                            basArea=list(old_name="basalArea_m2.ha",new_name="basalArea",unit="m^2 ha^-1",method="Pfeiffer survey"),
#                            length=list(old_name="length_m",new_name="channelLength",unit="m",method="Pfeiffer survey"),
#                            width=list(old_name="width_m",new_name="bankfullWidth",unit="m",method="Pfeiffer survey"),
#                            woodLod=list(old_name="woodLoad_m3",new_name="woodVol",unit="m^3",method="Pfeiffer survey"),
#                            coarse=list(old_name="coarseSed_m3",new_name="coarseSedVol",unit="m^3",method="Pfeiffer survey"),
#                            fine=list(old_name="fineSed_m3",new_name="fineSedVol",unit="m^3",method="Pfeiffer survey"),
#                            pom=list(old_name="pom_m3",new_name="pomVol",unit="m^3",method="Pfeiffer survey"),
#                            arealWood=list(old_name="woodPerArea",new_name="woodDepth",unit="m",method="Pfeiffer survey"),
#                            arealCoarse=list(old_name="coarseSedPerArea",new_name="coarseDepth",unit="m",method="Pfeiffer survey"),
#                            arealFine=list(old_name="fineSedPerArea",new_name="fineDepth",unit="m",method="Pfeiffer survey"),
#                            arealPom=list(old_name="pomPerArea",new_name="pomDepth",unit="m",method="Pfeiffer survey"))
# 
# pfeiffer=addUnitMethod(pfeiffer,name_unit_method_list)
# pfeiffer$QCStatusOK=TRUE
# pfeiffer=plyr::rename(pfeiffer,replace=c(x="X",y="Y"))
# pfeiffer$dateTime=as.Date("2016/8/1")
# addData(pfeiffer,
#         batchName = "Pfeiffer Thesis Data",
#         batchSource="C:/Users/Sam/Documents/LeakyRivers/Data/morph/pfeiffer_thesis_data.csv",
#         inEPSG=4326,
#         streamSnapDistCells=50)
# 

############--------------bob resp data---------------######
resp=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/resp/bob_respRates_simpleSites.csv",colClasses="character")
resp$areaPath=paste0("C:/Users/Sam/Documents/LeakyRivers/Data/resp/respShapes/",resp$areaName,".shp")
resp=melt(resp,id.vars=c("x","y","areaName","areaPath","dateTime"),
          measure.vars=c("Temperature","GPP","ER","K600"),
          variable.name="metric")
resp=plyr::rename(resp,replace=c(x="X",y="Y"))
resp$dateTime=as.Date(resp$dateTime)

name_unit_method_list=list(temp=list(old_name="Temperature",new_name="temperature",unit="deg C", method = "Bob metab survey"),
                           gpp=list(old_name="GPP",new_name="GPP",unit="mmol O2 m^-2 day^-1", method = "Bob metab survey"),
                           er=list(old_name="ER",new_name="ER",unit="mmol O2 m^-2 day^-1", method = "Bob metab survey"),
                           k=list(old_name="K600",new_name="K600",unit="day^-1",method="Bob metab survey"))
resp=addUnitMethod(resp,name_unit_method_list)
resp$QCStatusOK=T
addData(resp,
        batchName="Bob Metabolism Data",
        batchSource="C:/Users/Sam/Documents/LeakyRivers/Data/resp/bob_respRates_simpleSites.csv",
        inEPSG=4326,
        streamSnapDistCells=50)


#########---------------bridget morphology data----------------------###########
morph=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/morph/morphForDB.csv")

morph=morph[,c("Treatment","Reach","Network","NEW.Confinement","Mean.valley.width","Mean.width.of.ind..Channel","Mean.total.width..m.","Proportion.jams.with.pools","Jams","Length..m.","WoodVolPerArea","Up.Y","Up.X","Down.Y","Down.X","totalSedC_Mg_100m_valley","Valley.length..m.")]
morph$areaName=morph$Reach
morph$areaPath=paste0("C:/Users/Sam/Documents/LeakyRivers/Data/morph/morphShapes/",morph$areaName,".shp")

#I'm not sure if I trust this, leaving it in for now...
morph$meanNumberOfChannels=morph$Mean.total.width..m./morph$Mean.width.of.ind..Channel

#morph$totalChannelLength=morph$Length..m.*morph$meanNumberOfChannels

#morph$meanNumberOfChannels=round(morph$meanNumberOfChannels)

morph$JamsPerKm=morph$Jams*(1000/morph$Length..m.)
morph$JamPoolsPerKm=morph$JamsPerKm*morph$Proportion.jams.with.pools

morph$totalSedOCPerKm=morph$totalSedC_Mg_100m_valley*10/morph$Length..m.

morph=plyr::rename(morph,replace=c(Down.X="X",Down.Y="Y"))

morph=melt(morph,id.vars=c("areaName","areaPath","X","Y"),
           measure.vars = c("Treatment","NEW.Confinement","Mean.valley.width",
                            "Mean.width.of.ind..Channel","Mean.total.width..m.","Proportion.jams.with.pools",
                            "Jams","Length..m.","WoodVolPerArea","JamsPerKm","JamPoolsPerKm",
                            "totalSedOCPerKm","meanNumberOfChannels"),
           variable.name = "metric")

name_unit_method_list=list(treat=list(old_name="Treatment",new_name="landUse",unit="categorical",method="Bridget morphology survey"),
                           conf=list(old_name="NEW.Confinement",new_name="confinement",unit="categorical",method="Bridget morphology survey"),
                           vw=list(old_name="Mean.valley.width",new_name="valleyWidth",unit="m",method="Bridget morphology survey"),
                           cw=list(old_name="Mean.width.of.ind..Channel",new_name="individualBankfullWidth",unit="m",method="Bridget morphology survey"),
                           tcw=list(old_name="Mean.total.width..m.",new_name="bankfullWidth",unit="m",method="Bridget morphology survey"),
                           pjp=list(old_name="Proportion.jams.with.pools",new_name="jamPoolProportion",unit="jamPools/jam",method="Bridget morphology survey"),
                           jam=list(old_name="Jams",new_name="jamCount",unit="count",method="Bridget morphology survey"),
                           len=list(old_name="Length..m.",new_name="channelLength",unit="m",method="Bridget morphology survey"),
                           wva=list(old_name="WoodVolPerArea",new_name="woodDepth",unit="m",method="Bridget morphology survey"),
                           chc=list(old_name="meanNumberOfChannels",new_name="channelCount",unit="count",method="Bridget morphology survey"),
                           jpk=list(old_name="JamsPerKm",new_name="jamsPerKm",unit="count km^-1",method="Bridget morphology survey"),
                           jppk=list(old_name="JamPoolsPerKm",new_name="jamPoolsPerKm",unit="count km^-1",method="Bridget morphology survey"),
                           #tcl=list(old_name="totalChannelLength",new_name="totalChannelLength",unit="m",method="Bridget morphology survey"),
                           sopk=list(old_name="totalSedOCPerKm",new_name="sedOCPerKm",unit = "Kg stream sediment C km^-1",method="Bridget morphology survey"))
morph=addUnitMethod(morph,name_unit_method_list)
morph$dateTime=as.Date("2015/8/1")
morph$QCStatusOK=T
addData(morph,
        batchName="Bridget Geomorph Survey",
        batchSource="C:/Users/Sam/Documents/LeakyRivers/Data/morph/morphForDB.csv",
        inEPSG=4326,
        streamSnapDistCells=50)

################----------------------Whol&beckman 2014 data-----------------------
morph=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/morph/wholBeckmanLongitudnalJams_NSV_GC_2_13.csv")
morph$slopeDeg = atan(morph$slope) * (180 / pi)
morph$JamsPerKm=morph$jam.count/(morph$length/1000)
morph$areaName=paste0(morph$reach,morph$id)
morph$areaPath=paste0("C:/Users/Sam/Documents/LeakyRivers/Data/morph/WholBeckmanShapes/",morph$areaName,".shp")
morph=melt(morph,id.vars = c("areaName","areaPath"),
           measure.vars = c("stand.age","typ","width","JamsPerKm","slopeDeg","landUse","length","jam.count"),
           variable.name = "metric")
name_unit_method_list=list(treat=list(old_name="landUse",new_name="landUse",unit="categorical",method="Wohl Beckman 2014"),
                           sta=list(old_name="stand.age",new_name="standAge",unit="years",method="Wohl Beckman 2014"),
                           typ=list(old_name="typ",new_name="confinement",unit="categorical",method="Wohl Beckman 2014"),
                           width=list(old_name="width",new_name="bankfullWidth",unit="m",method="Wohl Beckman 2014"),
                           jpk=list(old_name="JamsPerKm",new_name="jamsPerKm",unit="count km^-1",method="Wohl Beckman 2014"),
                           slp=list(old_name="slopeDeg",new_name="slope",unit="degrees",method="Wohl Beckman 2014"),
                           chl=list(old_name="length",new_name="channelLength",unit="m",method="Wohl Beckman 2014"),
                           jam=list(old_name="jam.count",new_name="jamCount",unit="count",method="Wohl Beckman 2014"))
morph=addUnitMethod(morph,name_unit_method_list)
morph$dateTime=as.Date("2012/8/1")
morph$QCStatusOK=T
addData(morph,
        batchName="Wohl Beckman 2014",
        batchSource="C:/Users/Sam/Documents/LeakyRivers/Data/morph/wholBeckmanLongitudnalJams_NSV_GC.csv",
        inEPSG=4326,
        streamSnapDistCells=50,
        addMidpoint=T)



#####################---------------------add DEM derived points------------------------
segs=read.csv("StreamSegs_slope_conf_xxl.csv")
segs=melt(segs,id.vars=c("cat","X","Y"),
          measure.vars = c("slope","heading_rad","elevation","latRange_10","latRange_25","minLatRange_10","minLatRange_25","UAA","SPI","valleyWidth_05","valleyWidth_1","slope_25"),
          variable.name = "metric")
name_unit_method_list=list(slope=list(old_name="slope",new_name="slope",unit="degrees",method="derived from DEM"),
                           hea=list(old_name="heading_rad",new_name="azimuth",unit="radians",method="derived from DEM"),
                           elev=list(old_name="elevation",new_name="elevation",unit="m",method="derived from DEM"),
                           l1=list(old_name="latRange_10",new_name="latRange_10",unit="meters",method="derived from DEM"),
                           l2=list(old_name="latRange_25",new_name="latRange_25",unit="meters",method="derived from DEM"),
                           s2=list(old_name="slope_25",new_name="slope_25",unit="degrees",method="derived from DEM"),
                           ml1=list(old_name="minLatRange_10",new_name="minLatRange_10",unit="meters",method="derived from DEM"),
                           ml2=list(old_name="minLatRange_25",new_name="minLatRange_25",unit="meters",method="derived from DEM"),
                           ua=list(old_name="UAA",new_name="UAA",unit="km^2",method="derived from DEM"),
                           spi=list(old_name="SPI",new_name="SPI",unit="index",method="derived from DEM"),
                           vw=list(old_name="valleyWidth_05",new_name="valleyWidth_05",unit="meters",method="derived from DEM"),
                           vw=list(old_name="valleyWidth_1",new_name="valleyWidth_1",unit="meters",method="derived from DEM"))
segs=addUnitMethod(segs,name_unit_method_list)

segs$dateTime=as.Date("2018/12/1")
segs$QCStatusOK=T
addData(segs,
        batchName = "DEM derived metrics calculated by Sam",
        batchSource="createStreamSegsDF()")



###############----------add area characteristics to segment points
dbGetQuery(leakyDB,"SELECT * FROM DataTypes")
dbGetQuery(leakyDB,"SELECT * FROM Batches")


characterizePointsByAreas(pointsBatch=5,dataTypesToAdd=c(1:34))


###############----------add many metrics to areas

# characterizeAreas(areasBatchName="Bob Metabolism Data",addDTs=c(1:5,24:58),newBatchName = "mean of segPoint values")
# characterizeAreas(areasBatchName = "Bridget Geomorph Survey",addDTs=c(1:5,20:23,39:58),newBatchName="mean of segPoint values")
# characterizeAreas(areasBatchName="Wohl Beckman 2014",addDTs=c(1:5,20:38,47:58),newBatchName = "mean of segPoint values")
# characterizeAreas(areasBatchName="mikeSamWidths",addDTs = c(20:58),newBatchName = "mean of segPoint values")

characterizeAreas(areasBatchName="Bob Metabolism Data",addDTs=c(1:46),newBatchName = "mean of segPoint values")
characterizeAreas(areasBatchName = "Bridget Geomorph Survey",addDTs=c(1:46),newBatchName="mean of segPoint values")
characterizeAreas(areasBatchName="Wohl Beckman 2014",addDTs=c(1:46),newBatchName = "mean of segPoint values")
characterizeAreas(areasBatchName="mikeSamWidths",addDTs = c(1:46),newBatchName = "mean of segPoint values")


###############----------add watershedID to locations---------------------################
inWatershed(watershedIDs = dbGetQuery(leakyDB,"SELECT WatershedID FROM watersheds")$watershedID)
