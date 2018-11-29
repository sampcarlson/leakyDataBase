require(tidyverse)
library(reshape2)
library(RSQLite)
require(sp)
library(sf)
require(raster)
require(rgdal)
require(rgrass7)
require(rgeos)
require(reshape2)

source("C:/Users/Sam/Documents/R Projects/BuildLeakyDB/leakyFunctions.R")
source('~/R Projects/mapC/grassTools.R')

leakyDB=dbConnect(SQLite(),"C:\\Users\\Sam\\Documents\\LeakyRivers\\Data\\sqLiteDatabase\\LeakyData.db3")
dbListTables(leakyDB)

#remove all data:
lapply(dbListTables(leakyDB),FUN=delete_data,db=leakyDB)


###################-------------write networks table, associated coordinates, and types----------
dbListFields("Networks",conn=leakyDB)
NetworksFrame=data.frame(NetworkIDX=c(1:5),NetworkName=c("NSV","GC","SFP","Snowy","Other"),OutflowCoordID=c(1:5))
dbWriteTable(conn=leakyDB,name="Networks",value=NetworksFrame,append=T)
#dbGetQuery(leakyDB,"Select * from Networks")


dbListFields(leakyDB,"CoordinateTypes")
dbWriteTable(leakyDB,"CoordinateTypes", value=data.frame(CoordinateTypeIDX=c(1,2),Description=c("OutflowPoint","Midpoint")),append=T)
#dbGetQuery(leakyDB,"Select * from CoordinateTypes")


dbListFields(leakyDB,"Coordinates")
NetworksCoordsFrame=data.frame(CoordinateIDX=c(1:3),Name=c("NSV_out","GC_out","SFP_out"),
                               CoordinateTypeIDX=rep(1,3),X=c(452943.846,446016.664,455567.336),Y=c(4451309.401,4462604.065,4490220.298),
                               EPSG=rep(32613,3))
dbWriteTable(leakyDB,"Coordinates",value=NetworksCoordsFrame,append=T)
dbGetQuery(leakyDB,"Select * from Coordinates")






###################----------------wrangle some width data-----------
rawWidth=read_csv("C:/Users/Sam/Dropbox/Logjams/R_geomorph/wrangleData/width/CoWidths.csv")[-1,]
rawSites=read_csv("C:/Users/Sam/Dropbox/Logjams/R_geomorph/wrangleData/width/widthSites.csv")

#check that all width sites are in sites list:
all((unique(rawWidth$Site) %in% unique(rawSites$Sites)))

#add dep area as pct, and remove extra data
keepNames=c("Date","Site","Transect","Wetted width","Bank-full width","Depositional area units","Channel type","Device","Who","Comments","DepArea_pct")
widths=calcDepPct(rawWidth)[keepNames]
widths$ReachName=widths$Site
widths$PointName=paste(widths$ReachName,widths$Transect,sep=' #')
widths=left_join(widths,rawSites[c("Sites","X","Y","Network")],by=c("ReachName" = "Sites"))
widths=left_join(widths, dbGetQuery(leakyDB,"Select NetworkIDX, NetworkName from Networks"),by=c("Network"="NetworkName"))
w_long=as.tibble(melt(widths,measure.vars=c("Wetted width","Bank-full width","DepArea_pct")))
w_long$Value=as.numeric(w_long$value)
w_long=w_long[!is.na(w_long$Value),]
w_long$dataType=""
w_long$unit=""
w_long$dataType[w_long$variable=="DepArea_pct"]=w_long$`Depositional area units`[w_long$variable=="DepArea_pct"]
w_long$unit[w_long$variable=="DepArea_pct"]='percent cover'
w_long$dataType[w_long$variable!="DepArea_pct"]=w_long$Device[w_long$variable!="DepArea_pct"]
w_long$unit[w_long$variable!="DepArea_pct"]='meters'
w_long$dataType[w_long$dataType=='m2']='Estimated as total area'
w_long$dataType[w_long$dataType=='%']='Estimated as pct cover'


#################-----------------load width data----------------
#writeIfNew(db,tableName,df,compareNames,IDXName)

#specify / check these:
batchName ="CoWidths.csv"
thisBatchID=addBatch(batchName)

writeIfNew(leakyDB,"QCStatus",data.frame(Description='ok'),"Description","QCStatusIDX")

dataTypes=unique(w_long[c("variable","dataType","unit")])
names(dataTypes)=c("Metric","Method","Unit")
writeIfNew(leakyDB,"DataTypes",dataTypes,c("Metric","Unit","Method"),"DataTypeIDX")
#get types back to join with the data
dataTypes=dbGetQuery(leakyDB,"Select * From DataTypes")
w_long=left_join(w_long,dataTypes,by=c("variable"="Metric","dataType"="Method","unit"="Unit"))

w_long$sensor='Homo Sapiens'
w_long$sensor[w_long$dataType=="Laser"]="Laser Rangefinder"
sensors=unique(w_long$sensor)
writeIfNew(leakyDB,"Sensors",data.frame(SensorType=sensors,SerialNumber=c("unknown","AGA GTT TGA TCC TGG CTC AG")),c("SensorType","SerialNumber"),"SensorIDX")

sources=unique(w_long[c("sensor","Who")])
sources=left_join(sources,dbGetQuery(leakyDB,"SELECT * FROM Sensors"), by=c("sensor"="SensorType"))
names(sources)[names(sources)=="Who"]="Person"
writeIfNew(leakyDB,"Sources",sources[c("SensorIDX","Person")],c("SensorIDX","Person"),"SourceIDX")
#join source back to dataset
sources=dbGetQuery(leakyDB,"Select * From Sources")
sources=left_join(sources,dbGetQuery(leakyDB,"Select SensorIDX, SensorType From Sensors"))
w_long=left_join(w_long,sources,by=c("Who"="Person","sensor"="SensorType"))

# There is only one type (midpoint) of coords here.  However, some reaches have upstream and downstream points.  
# This is why CoordinateIDX and rowIDX are seperate 
coords=unique(w_long[c("ReachName","X","Y")])
coords$Type="Midpoint"
coords$EPSG=4326
coords=left_join(coords,dbGetQuery(leakyDB,"Select * From CoordinateTypes"),by=c("Type"="Description"))
#define coordinateIDX (rowIDX in coordinates comes when adding to table)
reachCoords=data.frame(ReachName=unique(coords$ReachName))
reachCoords$CoordinateIDX=(1:nrow(reachCoords))+max(as.numeric(dbGetQuery(leakyDB,"SELECT MAX(CoordinateIDX) FROM Coordinates")),0,na.rm = T)
coords=left_join(coords,reachCoords)
coords$Name=paste(coords$ReachName,coords$Type)
writeIfNew(leakyDB,"Coordinates",coords,c("CoordinateIDX","Name","CoordinateTypeIDX","X","Y","EPSG"),"RowIDX")

#reaches
reachCoords$LandUse="Turkey Ranch" # data from another source, this is a placeholder (unless RMNP has changed quite a bit since I was there last)
reachCoords$Confinement=''  #no data yet
reaches=left_join(reachCoords,base::unique(w_long[c("Site","NetworkIDX")]),by=c("ReachName"="Site"))
reaches$IsReal=TRUE
writeIfNew(leakyDB,"Reaches",reaches,c("ReachName","CoordinateIDX","LandUse","Confinement","NetworkIDX","IsReal"),"ReachIDX")

#points
points=unique(w_long[c("PointName","ReachName")])
points=left_join(points,dbGetQuery(leakyDB,"SELECT ReachIDX, ReachName FROM Reaches"))
points$IsReal=FALSE
writeIfNew(leakyDB,"Points",points,c("PointName","ReachIDX"),"PointIDX")


#write some data!
#my function is picky about names, so I'm adding some extra collumns to match
w_long$DateTime=w_long$Date
w_long=left_join(w_long,dbGetQuery(leakyDB,"SELECT PointIDX, PointName FROM Points"))
w_long$BatchIDX=thisBatchID
w_long$QCStatusIDX=1
w_long$DataIDX=(1:nrow(w_long))+max(as.numeric(dbGetQuery(leakyDB,"Select MAX(DataIDX) FROM Data")),0,na.rm=T)
dbWriteTable(leakyDB,"Data",w_long[c("DataIDX","BatchIDX","DataTypeIDX","PointIDX","SourceIDX","DateTime","QCStatusIDX","Value")],append=T)


########-----------------Calc whole area segment slope, uaa, and confinement, add to db-------------
createStreamSegsDF=function(){
  streamSegs=shapefile("C:/Users/Sam/Documents/R Projects/mapC/xxl_streamSegs_250m.shp")
  #as compared to Q, the data is stored in attributes, and the attributes are stored as data
  #field 'AUTO' is unique segment ID
  
  #drop segments shorter than 2 coordinate pairs
  streamSegs=subset(streamSegs,sapply(streamSegs@lines,getCoordCount)>1)
  
  #Function (below) from rgeos works
  streamSegs$length=gLength(streamSegs,byid=T)
  
  InitGrass_byRaster("C:/Users/Sam/Desktop/spatial/QgisEnvironment/Inputs_and_scripts/allDemRaw/all4_wgs84_13n.tif")
  
  writeVECT(streamSegs,vname="streamSegs",v.in.ogr_flags = "o")
  execGRASS("v.rast.stats",map="streamSegs",raster="dem",column_prefix="elevRange",method="range")
  streamSegs=readVECT("streamSegs")
  streamSegs$slope=atan(streamSegs$elevRange_range/streamSegs$length) * (180/pi) #as percent
  streamSegs$X=1
  #hist(streamSegs$slope)
  #looks reasonable?
  
  getMidpointCoords=function(singleSegment,coord="XY"){
    mid_idx=round(length(singleSegment@Lines[[1]]@coords[,1])/2)
    if(coord=="X"){
      return(singleSegment@Lines[[1]]@coords[mid_idx,1])
    }
    if(coord=="Y"){
      return(singleSegment@Lines[[1]]@coords[mid_idx,2])
    }
    if(coord=="XY"){
      return(c(singleSegment@Lines[[1]]@coords[mid_idx,1],
               singleSegment@Lines[[1]]@coords[mid_idx,2]))
    }
    
  }
  
  getHeadings=function(feature,smoothScope=1){
    getSegHeadings=function(seg,smoothScope){
      n=nrow(seg@Lines[[1]]@coords)
      if(n>1){
        coordDif=seg@Lines[[1]]@coords[1,]-seg@Lines[[1]]@coords[n,]
        thisHeading=atan2(y=coordDif[2],x=coordDif[1])
        # for(i in 1:(nrow(seg@Lines[[1]]@coords)-1)){
        #   coordDif=seg@Lines[[1]]@coords[(i+1),]-seg@Lines[[1]]@coords[i,]
        #   thisHeading=atan2(y=coordDif[2],x=coordDif[1])
      }
      return(thisHeading)
    }
    
    headings=sapply(feature@lines,getSegHeadings,smoothScope=smoothScope)
    feature$heading_rad=headings
    return(feature)
  }
  
  streamSegs$X=sapply(streamSegs@lines,getMidpointCoords,coord="X")
  streamSegs$Y=sapply(streamSegs@lines,getMidpointCoords,coord="Y")
  streamSegs=getHeadings(streamSegs)
  
  streamSegsDF=as.data.frame(streamSegs[,c("cat","X","Y","slope","heading_rad")])
  
  rng=function(...){
    mi=min(...)
    ma=max(...)
    return(ma-mi)
  }
  dem_rast=raster("C:/Users/Sam/Desktop/spatial/QgisEnvironment/Inputs_and_scripts/allDemRaw/all4_wgs84_13n.tif")
  
  streamSegsDF$elevRange_25=raster::extract(x=dem_rast,
                                            y=streamSegsDF[,c("X","Y")],
                                            buffer=25,
                                            fun=rng)
  
  
  streamSegsDF$elevation=raster::extract(x=dem_rast,
                                         y=streamSegsDF[,c("X","Y")])
  
  getLateralElevRange=function(streamSegsDF,conf_range){
    streamSegsDF$latRange=0
    for(i in 1:nrow(streamSegsDF)){
    #  tan(streamSegsDF$heading_rad[i])
      left_y=(sin(streamSegsDF$heading_rad[i]+(pi/2))*conf_range)+streamSegsDF$Y[i]
      left_x=(cos(streamSegsDF$heading_rad[i]+(pi/2))*conf_range)+streamSegsDF$X[i]
      left_elev=raster::extract(x=dem_rast,y=data.frame(X=left_x,Y=left_y))
      right_y=(sin(streamSegsDF$heading_rad[i]-(pi/2))*conf_range)+streamSegsDF$Y[i]
      right_x=(cos(streamSegsDF$heading_rad[i]-(pi/2))*conf_range)+streamSegsDF$X[i]
      right_elev=raster::extract(x=dem_rast,y=data.frame(X=right_x,Y=right_y))
      streamSegsDF$latRange[i]=rng(c(left_elev,right_elev,streamSegsDF$elevation[i]))
    }
    return(streamSegsDF)
  }
  
  streamSegsDF=getLateralElevRange(streamSegsDF,conf_range = 10)
  names(streamSegsDF)[names(streamSegsDF)=="latRange"]="latRange_10"
  
  
  streamSegsDF=getLateralElevRange(streamSegsDF,conf_range = 25)
  names(streamSegsDF)[names(streamSegsDF)=="latRange"]="latRange_25"
  
  streamSegsDF=getLateralElevRange(streamSegsDF,conf_range = 50)
  names(streamSegsDF)[names(streamSegsDF)=="latRange"]="latRange_50"
  
  streamSegsDF$UAA=raster::extract(x=raster("C:/Users/Sam/Documents/R Projects/BuildLeakyDB/flowAccum_xxl.tif"),
                                  y=streamSegsDF[,c("X","Y")],buffer=25,fun=max)
  streamSegsDF$SPI=raster::extract(x=raster("C:/Users/Sam/Documents/R Projects/BuildLeakyDB/streamPower_xxl.tif"),
                                   y=streamSegsDF[,c("X","Y")],buffer=25,fun=max)
  
  write.csv(streamSegsDF,"StreamSegs_slope_conf_xxl.csv")
}

#go have lunch & a beer or two - this takes a long time
#createStreamSegsDF()
#--here--------------------
#check for SPI, add to metrics below...

streamSegsDF=read.csv("StreamSegs_slope_conf_xxl.csv")

streamSegsDF$segID=1:nrow(streamSegsDF)

#make two tibbles, location and characteristics, in long format, 
reachCoords=streamSegsDF[,c("segID","X","Y")]
reachCoords$type="Midpoint"
reachData=melt(as.tibble(streamSegsDF[,c("segID","elevRange_25","slope","elevation","latRange_10","latRange_25","latRange_50","UAA","SPI")]),
               id.vars="segID")

rm(streamSegsDF)

#####--------------write to leakyDB-------------
batchName ="rGis_slope_confinement"
thisBatchID=addBatch(batchName)

writeIfNew(leakyDB,"QCStatus",data.frame(Description='ok'),"Description","QCStatusIDX")
qcStatusOK=dbGetQuery(leakyDB,"SELECT QCStatusIDX FROM QCStatus WHERE Description = 'ok'")[[1]]

#write coordinate types and metrics (if new)

writeIfNew(leakyDB,"CoordinateTypes",data.frame(Description=unique(reachCoords$type)),"Description","CoordinateTypeIDX")
#join coord types back to coords
reachCoords=left_join(reachCoords,dbGetQuery(leakyDB,"SELECT * FROM CoordinateTypes"),by=c("type"="Description"))

writeIfNew(leakyDB,"DataTypes",data.frame(Metric=c("elevRange_25","slope","elevation","latRange_10","latRange_25","latRange_50","UAA","SPI"),
                                          Unit=c("meters","percent","meters","meters/10 meters","meters/25 meters","meters/50 meters","m^2","index"),Method="GRASS"),c("Metric","Unit","Method"),"DataTypeIDX")
#join data types back to data
reachData=left_join(reachData,dbGetQuery(leakyDB,"SELECT Metric, DataTypeIDX FROM DataTypes"),by=c("variable"="Metric"))

#write coordinates
reachCoords$CoordinateIDX=as.numeric(reachCoords$segID)+max(as.numeric(dbGetQuery(leakyDB,"SELECT MAX(CoordinateIDX) FROM Coordinates")),0,na.rm = T)
reachCoords$EPSG=32613
writeIfNew(leakyDB,"Coordinates",data.frame(CoordinateIDX=reachCoords$CoordinateIDX,
                                            Name=paste0("AllSegs_",reachCoords$segID),
                                            CoordinateTypeIDX=reachCoords$CoordinateTypeIDX,
                                            X=reachCoords$X,
                                            Y=reachCoords$Y,
                                            EPSG=reachCoords$EPSG),
           c("Name","X","Y"),"RowIDX")

#reaches
dbGetQuery(leakyDB,"SELECT NetworkIDX FROM Networks WHERE NetworkName = 'Other' ")[[1]]
reaches=data.frame(ReachName=paste0("AllSegs_",reachCoords$segID),
                   CoordinateIDX=reachCoords$CoordinateIDX,
                   LandUse="Turkey Ranch",Confinement='',
                   NetworkIDX=dbGetQuery(leakyDB,"SELECT NetworkIDX FROM Networks WHERE NetworkName = 'Other' ")[[1]],
                   IsReal=TRUE)
reaches=base::unique(reaches)

writeIfNew(leakyDB,"Reaches",reaches,c("ReachName","CoordinateIDX","LandUse","Confinement","NetworkIDX","IsReal"),"ReachIDX")
#join reach idx back to reaches
reaches_idx=data.frame(ReachName=reaches$ReachName)
reaches_idx=left_join(reaches_idx,dbGetQuery(leakyDB,"Select ReachIDX, ReachName FROM Reaches"))

#points
points=data.frame(ReachIDX=reaches_idx$ReachIDX,IsReal=FALSE,PointName="ReachMidpoint")
writeIfNew(leakyDB,"Points",points,c("PointName","ReachIDX"),"PointIDX")

#sensors, sources, data
writeIfNew(leakyDB,"Sensors",data.frame(SensorType="GRASS GIS",SerialNumber=7),"SensorType","SensorIDX")
thisSensor=dbGetQuery(leakyDB,"SELECT SensorIDX FROM Sensors WHERE SensorType = 'GRASS GIS'")[[1]]

writeIfNew(leakyDB,"Sources",data.frame(SensorIDX=thisSensor,Person="Sam Carlson"),c("SensorIDX","Person"),"SourceIDX")
thisSource=dbGetQuery(leakyDB,paste0("SELECT SourceIDX FROM Sources WHERE SensorIDX = '",thisSensor,"'"))

reachData=left_join(reachData,
                    left_join(reachCoords[c("segID","CoordinateIDX")],
                              left_join(dbGetQuery(leakyDB,"SELECT PointIDX, ReachIDX From Points"),dbGetQuery(leakyDB,"SELECT ReachIDX, CoordinateIDX From Reaches"))
                    )
)

writeData=data.frame(BatchIDX=thisBatchID,
                     DataTypeIDX=reachData$DataTypeIDX,
                     PointIDX=reachData$PointIDX,
                     SourceIDX=thisSource,
                     DateTime='',
                     QCStatusIDX=qcStatusOK,
                     Value=reachData$value)
writeData=writeData[complete.cases(writeData),]
dbWriteTable(leakyDB,"Data",writeData,append=T)

#######----read and write pfeiffer data------------------

pf_raw=read.csv("pfeiffer_thesis_data.csv")

splitOnComma=function(inString,which){
  inString=as.character(inString)
  if(which=='x'){return(paste0("-",strsplit(inString,", ")[[1]][2]))}
  if(which=='y'){return(strsplit(inString,", ")[[1]][1])}
}

pf_raw$x=sapply(pf_raw$coordinates,splitOnComma,which='x')
pf_raw$y=sapply(pf_raw$coordinates,splitOnComma,which='y')


pf_raw$woodPerArea=pf_raw$woodLoad_m3 /(pf_raw$length_m *pf_raw$width_m )
pf_raw$coarseSedPerArea=pf_raw$coarseSed_m3 /(pf_raw$length_m *pf_raw$width_m )
pf_raw$fineSedperArea=pf_raw$fineSed_m3 /(pf_raw$length_m *pf_raw$width_m )
pf_raw$pomPerArea=pf_raw$pom_m3 /(pf_raw$length_m *pf_raw$width_m )


#names(pf_raw)
#split coords from data, use name as id:
pf_location=pf_raw[c("Name","x","y")]
pf_data=pf_raw[c("Name","gradient_deg","confinementRatio","width_m","woodPerArea","coarseSedPerArea","fineSedperArea","pomPerArea")]
names(pf_data)=c("Name","slope","Confinement","Bank-full width","Wood depth", "Coarse Sed Volume","Fine Sed Volume", "POM Volume")
pf_data$Confinement[pf_data$Confinement=="*"]=NA
pf_data=melt(pf_data,id.vars="Name",na.rm=T,factorsAsStrings = T)

#####--------------write to leakyDB-------------
#source("C:/Users/Sam/Dropbox/Logjams/R_geomorph/wrangleData/leakyFunctions.R")

#leakyDB=dbConnect(SQLite(),"C:\\Users\\Sam\\Documents\\LeakyRivers\\Data\\sqLiteDatabase\\LeakyData.db3")
#dbListTables(leakyDB)

batchName ="pfeiffer_whol_geomorph"
thisBatchID=addBatch(batchName)

writeIfNew(leakyDB,"QCStatus",data.frame(Description='ok'),"Description","QCStatusIDX")
qcStatusOK=dbGetQuery(leakyDB,"SELECT QCStatusIDX FROM QCStatus WHERE Description = 'ok'")[[1]]

#coordinate types and metrics (write if new)
coordType=dbGetQuery(leakyDB,"SELECT CoordinateTypeIDX FROM CoordinateTypes WHERE Description = 'Midpoint'")[[1]]
pf_location$CoordinateTypeIDX=coordType
#unique(pf_data$variable)
#dbGetQuery(leakyDB,"SELECT * FROM DataTypes")
dataTypes=data.frame(Metric=c("slope","Confinement","Bank-full width","Wood depth","Coarse Sed Volume","Fine Sed Volume","POM Volume"),
                     Unit=c("percent","width ratio","meters","m^3/m^2","m^3/m^2","m^3/m^2","m^3/m^2"),
                     Method=c("Field Observation","Field Observation","Tape","Field Observation","Field Observation","Field Observation","Field Observation"))


writeIfNew(leakyDB,"DataTypes",dataTypes,c("Metric","Unit","Method"),"DataTypeIDX")
#join data types back to data
dataTypes=left_join(dataTypes, dbGetQuery(leakyDB,"SELECT * FROM DataTypes"))
pf_data=left_join(pf_data,dataTypes,by=c("variable"="Metric"))

#write coordinates
pf_location$CoordinateIDX=1:nrow(pf_location)+max(as.numeric(dbGetQuery(leakyDB,"SELECT MAX(CoordinateIDX) FROM Coordinates")),0,na.rm = T)
pf_location$EPSG=4326
writeIfNew(leakyDB,"Coordinates",data.frame(CoordinateIDX=pf_location$CoordinateIDX,
                                            Name=paste0("Pfeiffer_reach_",pf_location$Name),
                                            CoordinateTypeIDX=pf_location$CoordinateTypeIDX,
                                            X=pf_location$x,
                                            Y=pf_location$y,
                                            EPSG=pf_location$EPSG)
           ,c("Name","X","Y"),"RowIDX")



#reaches
nsvID=dbGetQuery(leakyDB,"SELECT NetworkIDX FROM Networks WHERE NetworkName = 'NSV' ")[[1]]
reaches=data.frame(ReachName=paste("Pfeiffer_reach_",pf_location$Name),
                   CoordinateIDX=pf_location$CoordinateIDX,
                   LandUse="Turkey Ranch",Confinement='',
                   NetworkIDX=nsvID,
                   IsReal=TRUE)
reaches=base::unique(reaches)

writeIfNew(leakyDB,"Reaches",reaches,c("ReachName","CoordinateIDX","LandUse","Confinement","NetworkIDX","IsReal"),"ReachIDX")
#join reach idx back to reaches
reaches_idx=data.frame(ReachName=reaches$ReachName)
reaches_idx=left_join(reaches_idx,dbGetQuery(leakyDB,"Select ReachIDX, ReachName FROM Reaches"))

#points
points=data.frame(ReachIDX=reaches_idx$ReachIDX,IsReal=FALSE,PointName="FakeReachPoint")
writeIfNew(leakyDB,"Points",points,c("PointName","ReachIDX"),"PointIDX")

#sensor
thisSensor=dbGetQuery(leakyDB,"SELECT SensorIDX FROM Sensors WHERE SensorType = 'Homo Sapiens'")
writeIfNew(leakyDB,"Sources",data.frame(SensorIDX=thisSensor,Person="Andrew Pfeiffer"),c("SensorIDX","Person"),"SourceIDX")
thisSource=dbGetQuery(leakyDB,paste0("SELECT SourceIDX FROM Sources WHERE SensorIDX = '",thisSensor,"' AND Person = 'Andrew Pfeiffer'"))

pf_data=left_join(pf_data,
                  left_join(pf_location[c("Name","CoordinateIDX")],
                            left_join(dbGetQuery(leakyDB,"SELECT PointIDX, ReachIDX From Points"),dbGetQuery(leakyDB,"SELECT ReachIDX, CoordinateIDX From Reaches"))
                  )
)

writeData=data.frame(BatchIDX=thisBatchID,
                     DataTypeIDX=pf_data$DataTypeIDX,
                     PointIDX=pf_data$PointIDX,
                     SourceIDX=thisSource,
                     DateTime='',
                     QCStatusIDX=qcStatusOK,
                     Value=pf_data$value)

dbWriteTable(leakyDB,"Data",writeData,append=T)


##########------------read and load bob resp data---------

#respData=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/resp/bob_respRates.csv",colClasses = "character")
#respSites=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/resp/masterSites_bobResp.csv",colClasses = "character")

respData=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/resp/bob_respRates_simpleSites.csv",colClasses="character")
#exclude snowy sites
#respData=respData[! respData$network == "Snowy",]
respSites=respData[,c("site","type","y","x","network")]
respData=respData[,c("site","GPP","ER","Temperature")]

respData=melt(respData,id.vars = "site")


batchName ="Bob_Resp"
thisBatchID=addBatch(batchName)

writeIfNew(leakyDB,"QCStatus",data.frame(Description='ok'),"Description","QCStatusIDX")
qcStatusOK=dbGetQuery(leakyDB,"SELECT QCStatusIDX FROM QCStatus WHERE Description = 'ok'")[[1]]

#coordinate types and metrics (write if new)
dbGetQuery(leakyDB,"SELECT * FROM CoordinateTypes")
dbWriteTable(leakyDB,"CoordinateTypes", value=data.frame(CoordinateTypeIDX=c(3,4),Description=c("UpstreamPoint","DownstreamPoint")),append=T)

respSites=left_join(respSites,dbGetQuery(leakyDB,"SELECT CoordinateTypeIDX, Description FROM CoordinateTypes "),by=c("type"="Description"))


unique(respData$variable)
writeIfNew(leakyDB,"DataTypes",data.frame(Metric=c("GPP","ER","Temperature")
                                          ,Unit=c("mmol O2 m-2 d-1","mmol O2 m-2 d-1","degrees C"),Method="Bob"),c("Metric","Unit","Method"),"DataTypeIDX")
#join data types back to data
respData=left_join(respData,dbGetQuery(leakyDB,"SELECT Metric, DataTypeIDX FROM DataTypes"),by=c("variable"="Metric"))

#assign coord idx
respSites_temp=data.frame(site=unique(respSites$site))
respSites_temp$CoordinateIDX=(1:nrow(respSites_temp))+max(as.numeric(dbGetQuery(leakyDB,"SELECT MAX(CoordinateIDX) FROM Coordinates")),0,na.rm = T)
respSites=left_join(respSites,respSites_temp)

#write coordinates
respSites$EPSG=4269
writeIfNew(leakyDB,"Coordinates",data.frame(CoordinateIDX=respSites$CoordinateIDX,
                                            Name=paste0("RespSite_",respSites$site),
                                            CoordinateTypeIDX=respSites$CoordinateTypeIDX,
                                            X=respSites$x,
                                            Y=respSites$y,
                                            EPSG=respSites$EPSG)
           ,c("Name","X","Y"),"RowIDX")

#reaches
respSites=left_join(respSites, dbGetQuery(leakyDB,"SELECT NetworkName, NetworkIDX FROM Networks "),by=c("network"="NetworkName"))
reaches=data.frame(ReachName=paste0("RespSite_",respSites$site),
                   CoordinateIDX=respSites$CoordinateIDX,
                   LandUse="Turkey Ranch",Confinement='',
                   NetworkIDX=respSites$NetworkIDX,
                   IsReal=TRUE)
reaches=base::unique(reaches)

writeIfNew(leakyDB,"Reaches",reaches,c("ReachName","CoordinateIDX","IsReal"),"ReachIDX")
#join reach idx back to reaches
reaches_idx=data.frame(ReachName=reaches$ReachName)
reaches_idx=left_join(reaches_idx,dbGetQuery(leakyDB,"Select ReachIDX, ReachName FROM Reaches"))

#points
points=data.frame(ReachIDX=reaches_idx$ReachIDX,IsReal=FALSE,PointName="FakeReachPoint")
writeIfNew(leakyDB,"Points",points,c("PointName","ReachIDX"),"PointIDX")

#sensors, sources, data
writeIfNew(leakyDB,"Sensors",data.frame(SensorType="HOBO DO",SerialNumber=''),"SensorType","SensorIDX")
thisSensor=dbGetQuery(leakyDB,"SELECT SensorIDX FROM Sensors WHERE SensorType = 'HOBO DO'")[[1]]

writeIfNew(leakyDB,"Sources",data.frame(SensorIDX=thisSensor,Person="Bob Hall"),c("SensorIDX","Person"),"SourceIDX")
thisSource=dbGetQuery(leakyDB,paste0("SELECT SourceIDX FROM Sources WHERE Person = 'Bob Hall' AND SensorIDX = '",thisSensor,"'"))[[1]]


respData=unique(left_join(respData,
                          left_join(respSites[c("site","CoordinateIDX")],
                                    left_join(dbGetQuery(leakyDB,"SELECT PointIDX, ReachIDX From Points"),dbGetQuery(leakyDB,"SELECT ReachIDX, CoordinateIDX From Reaches"))
                          )
))

writeData=data.frame(BatchIDX=thisBatchID,
                     DataTypeIDX=respData$DataTypeIDX,
                     PointIDX=respData$PointIDX,
                     SourceIDX=thisSource,
                     DateTime='',
                     QCStatusIDX=qcStatusOK,
                     Value=respData$value)
dbWriteTable(leakyDB,"Data",writeData,append=T)



# #############-----------------------read spatial data, calculate stream temp--------------------
# lf_el=read.csv("lakeFractionAndElevation.csv", header=F)
# #seperate redundant ids from gc and nsv networks this is very brittle, but works for now
# lf_el$network=c(rep(2,29*2),rep(1,149*2-1))
# 
# elev_uaa=read.csv("nsv_gc_elev_uaa.csv",header=T)
# elev_uaa$network=c(rep(2,29*2),rep(1,149*2-1))
# 
# #convert uaa from pixels to km2
# elev_uaa$uaa=elev_uaa$uaa*((9.14308^2)/(1000^2))
# 
# elev_uaa_gc=elev_uaa[elev_uaa$network==2,]
# elev_uaa_nsv=elev_uaa[elev_uaa$network==1,]
# lf_el_gc=lf_el[lf_el$network==2,]
# lf_el_nsv=lf_el[lf_el$network==1,]
# 
# # calcTemp=function(lf,el,elev){
# #   return(lf*(-0.006*el+29.36)+(1-lf)*(-0.003*elev+16.337))
# # }
# calcTemp=function(lf,el,elev){
#   return(20.5+(3.414*lf)-(0.002119*elev)-(0.001889*el))
# }
# 
# 
# segTempData_gc=data.frame(SegIDX=unique(lf_el_gc$V1), lf=0, el=0, elev=0, uaa=0)
# for(s in segTempData_gc$SegIDX){
#   segTempData_gc$lf[segTempData_gc$SegIDX==s]=mean(lf_el_gc$V2[lf_el_gc$V1==s])
#   segTempData_gc$el[segTempData_gc$SegIDX==s]=mean(lf_el_gc$V3[lf_el_gc$V1==s])
#   segTempData_gc$elev[segTempData_gc$SegIDX==s]=mean(elev_uaa_gc$Elevation[elev_uaa_gc$pnt_val==s])
#   segTempData_gc$uaa[segTempData_gc$SegIDX==s]=mean(elev_uaa_gc$uaa[elev_uaa_gc$pnt_val==s])
# }
# 
# segTempData_nsv=data.frame(SegIDX=unique(lf_el_nsv$V1), lf=0, el=0, elev=0, uaa=0)
# for(s in segTempData_nsv$SegIDX){
#   segTempData_nsv$lf[segTempData_nsv$SegIDX==s]=mean(lf_el_nsv$V2[lf_el_nsv$V1==s])
#   segTempData_nsv$el[segTempData_nsv$SegIDX==s]=mean(lf_el_nsv$V3[lf_el_nsv$V1==s])
#   segTempData_nsv$elev[segTempData_nsv$SegIDX==s]=mean(elev_uaa_nsv$Elevation[elev_uaa_nsv$pnt_val==s])
#   segTempData_nsv$uaa[segTempData_nsv$SegIDX==s]=mean(elev_uaa_nsv$uaa[elev_uaa_nsv$pnt_val==s])
# }
# 
# calcTempWrap=function(inVect){
#   lf=inVect["lf"]
#   el=inVect["el"]
#   elev=inVect["elev"]
#   return(calcTemp(lf,el,elev))
# }
# 
# segTempData_gc$Temperature=apply(segTempData_gc,MARGIN=1,FUN=calcTempWrap)
# segTempData_nsv$Temperature=apply(segTempData_nsv,MARGIN=1,FUN=calcTempWrap)
# 
# ################----------------join to existing reachIDX's and load to db----------------
# 
# segTempData_gc$ReachName=paste0("GCmodel_ ",segTempData_gc$SegIDX)
# segTempData_nsv$ReachName=paste0("NSVmodel_ ",segTempData_nsv$SegIDX)
# segTempData=rbind(segTempData_gc,segTempData_nsv)
# 
# ###########---------------------this is fuckd, as it assumes reaches are already in db, and they are not-------------------
# dbReaches=dbGetQuery(leakyDB,"SELECT Reaches.ReachIDX, Reaches.ReachName FROM Reaches")
# segTempData=inner_join(segTempData,dbReaches)
# segTempData=segTempData[c("ReachIDX","Temperature", "lf", "elev", "uaa")]
# #clarify names
# names(segTempData)[names(segTempData)=="lf"]="Lake-Derived Fraction"
# names(segTempData)[names(segTempData)=="elev"]="Elevation"
# names(segTempData)[names(segTempData)=="uaa"]="UAA"
# 
# segTempData=melt(segTempData, id.vars="ReachIDX")
# 
# batchName ="Predicted Temperature"
# thisBatchID=addBatch(batchName)
# 
# writeIfNew(leakyDB,"QCStatus",data.frame(Description='ok'),"Description","QCStatusIDX")
# qcStatusOK=dbGetQuery(leakyDB,"SELECT QCStatusIDX FROM QCStatus WHERE Description = 'ok'")[[1]]
# 
# #coordinates and types, reaches are not new
# 
# 
# writeIfNew(leakyDB,"DataTypes",data.frame(Metric=c("Temperature", "Lake-Derived Fraction", "Elevation", "UAA")
#                                           ,Unit=c("degrees C","fraction","meters","pixel count"),Method="QGIS"),c("Metric","Unit","Method"),"DataTypeIDX")
# #join data types back to data
# segTempData=left_join(segTempData,dbGetQuery(leakyDB,"SELECT Metric, DataTypeIDX FROM DataTypes"),by=c("variable"="Metric"))
# 
# #points
# points=base::unique(data.frame(ReachIDX=segTempData$ReachIDX,IsReal=FALSE,PointName="FakeReachPoint"))
# writeIfNew(leakyDB,"Points",points,c("PointName","ReachIDX"),"PointIDX")
# 
# #sensors, sources, data
# writeIfNew(leakyDB,"Sensors",data.frame(SensorType="Model",SerialNumber='12'),"SensorType","SensorIDX")
# thisSensor=dbGetQuery(leakyDB,"SELECT SensorIDX FROM Sensors WHERE SensorType = 'Model'")[[1]]
# 
# writeIfNew(leakyDB,"Sources",data.frame(SensorIDX=thisSensor,Person="Sam Carlson"),c("SensorIDX","Person"),"SourceIDX")
# thisSource=dbGetQuery(leakyDB,paste0("SELECT SourceIDX FROM Sources WHERE Person = 'Sam Carlson' AND SensorIDX = '",thisSensor,"'"))[[1]]
# 
# segTempData=left_join(segTempData,dbGetQuery(leakyDB,paste0("SELECT PointIDX, ReachIDX FROM Points WHERE ReachIDX IN ( ",paste0(segTempData$ReachIDX, collapse=", ")," )")))
# 
# 
# writeData=data.frame(BatchIDX=thisBatchID,
#                      DataTypeIDX=segTempData$DataTypeIDX,
#                      PointIDX=segTempData$PointIDX,
#                      SourceIDX=thisSource,
#                      DateTime='',
#                      QCStatusIDX=qcStatusOK,
#                      Value=segTempData$value)
# writeData=writeData[complete.cases(writeData),]
# dbWriteTable(leakyDB,"Data",writeData,append=T)

#######----read and write Livers data - first source------------------
morph_raw=read.csv("C:\\Users\\Sam\\Documents\\LeakyRivers\\Data\\morph\\Geomorph_sites_and_data_bridget.csv")
morph_raw=morph_raw[,c("Treatment","Reach","Network","NEW.Confinement","Mean.valley.width","Mean.width.of.ind..Channel","Mean.total.width..m.","Proportion.jams.with.pools","Jams","Length..m.","WoodVolPerArea","Up.Y","Up.X","Down.Y","Down.X")]

morph_raw$Reach=paste0(morph_raw$Reach,"_",1:length(morph_raw$Reach))

#names(morph_raw)
#split coords from data, use reach as id:
morph_location_up=morph_raw[c("Reach","Up.X","Up.Y","Network")]
names(morph_location_up)=c("Reach","x","y","Network")
morph_location_up$type="UpstreamPoint"
morph_location_down=morph_raw[c("Reach","Down.X","Down.Y","Network")]
names(morph_location_down)=c("Reach","x","y","Network")
morph_location_down$type="DownstreamPoint"
morph_location=rbind(morph_location_up,morph_location_down)
rm(morph_location_up)
rm(morph_location_down)

#add some more things--
morph_raw$meanNumberOfChannels=morph_raw$Mean.total.width..m./morph_raw$Mean.width.of.ind..Channel
morph_raw$JamsPerKmChannel=morph_raw$Jams * (1000/morph_raw$Length..m.) / morph_raw$meanNumberOfChannels
morph_raw$JamPoolsPerKmChannel=morph_raw$JamsPerKmChannel*morph_raw$Proportion.jams.with.pools



morph_raw$Jams.km=morph_raw$Jams/(morph_raw$Length..m./1000)
morph_data=morph_raw[,c("Reach","Treatment","NEW.Confinement","Mean.valley.width","Mean.total.width..m.","WoodVolPerArea","JamsPerKmChannel","JamPoolsPerKmChannel")]
names(morph_data)=c("Name","Land Management", "Confinement",  "Valley width"     ,"Bank-full width"     ,"Wood depth"    ,"Jams per km"     ,"Jam pools per km")
morph_data=melt(morph_data,id.vars="Name",na.rm=T,factorsAsStrings = T)


#####--------------write to leakyDB-------------
batchName ="livers_whol_geomorph"
thisBatchID=addBatch(batchName)

writeIfNew(leakyDB,"QCStatus",data.frame(Description='ok'),"Description","QCStatusIDX")
qcStatusOK=dbGetQuery(leakyDB,"SELECT QCStatusIDX FROM QCStatus WHERE Description = 'ok'")[[1]]

#coordinate types and metrics
morph_location=left_join(morph_location, dbGetQuery(leakyDB,"SELECT * FROM CoordinateTypes"),by=c("type"="Description"))

#unique(morph_data$variable)
#dbGetQuery(leakyDB,"SELECT * FROM DataTypes")


dataTypes=data.frame(Metric=c("Land Management","Valley width","Jams per km",        "Wood depth","Bank-full width","Confinement", "Jam pools per km"),
                     Unit=c(  "categorical",    "meters",      "count / channel km", "m^3/m^2"   ,"meters"         ,"width ratio", "count / channel km" ),
                     Method=c("Field Observation","Field Observation","Field Observation","Field Observation","Tape","Field Observation","Field Observation"))


writeIfNew(leakyDB,"DataTypes",dataTypes,c("Metric","Unit","Method"),"DataTypeIDX")
#join data types back to data
dataTypes=left_join(dataTypes, dbGetQuery(leakyDB,"SELECT * FROM DataTypes"))
morph_data=left_join(morph_data,dataTypes,by=c("variable"="Metric"))




#coordinates
reachCoords=data.frame(Reach=base::unique(morph_location$Reach))
reachCoords$CoordinateIDX=1:nrow(reachCoords)+max(as.numeric(dbGetQuery(leakyDB,"SELECT MAX(CoordinateIDX) FROM Coordinates")),0,na.rm = T)
morph_location=left_join(morph_location,reachCoords)
morph_location$EPSG=4326
writeIfNew(leakyDB,"Coordinates",data.frame(CoordinateIDX=morph_location$CoordinateIDX,
                                            Name=paste0("livers_reach_",morph_location$Reach),
                                            CoordinateTypeIDX=morph_location$CoordinateTypeIDX,
                                            X=morph_location$x,
                                            Y=morph_location$y,
                                            EPSG=morph_location$EPSG)
           ,c("Name","X","Y"),"RowIDX")

#reaches
morph_location=left_join(morph_location,dbGetQuery(leakyDB,"SELECT * FROM Networks"),by=c("Network"="NetworkName"))
#morph_location

reaches=data.frame(ReachName=paste0("livers_reach_",morph_location$Reach),
                   CoordinateIDX=morph_location$CoordinateIDX,
                   LandUse="Turkey Ranch",Confinement='',
                   NetworkIDX=morph_location$NetworkIDX,
                   IsReal=TRUE)
reaches=base::unique(reaches)

writeIfNew(leakyDB,"Reaches",reaches,c("ReachName","CoordinateIDX","LandUse","Confinement","NetworkIDX","IsReal"),"ReachIDX")
#join reach idx back to reaches
reaches_idx=data.frame(ReachName=reaches$ReachName)
reaches_idx=left_join(reaches_idx,dbGetQuery(leakyDB,"Select ReachIDX, ReachName FROM Reaches"))

#points
points=data.frame(ReachIDX=reaches_idx$ReachIDX,IsReal=FALSE,PointName="FakeReachPoint")
writeIfNew(leakyDB,"Points",points,c("PointName","ReachIDX"),"PointIDX")

#sensor
thisSensor=dbGetQuery(leakyDB,"SELECT SensorIDX FROM Sensors WHERE SensorType = 'Homo Sapiens'")
writeIfNew(leakyDB,"Sources",data.frame(SensorIDX=thisSensor,Person="Bridget Livers"),c("SensorIDX","Person"),"SourceIDX")
thisSource=dbGetQuery(leakyDB,paste0("SELECT SourceIDX FROM Sources WHERE SensorIDX = '",thisSensor,"' AND Person = 'Bridget Livers'"))

morph_data=left_join(morph_data,
                     left_join(morph_location[c("Reach","CoordinateIDX")],
                               left_join(dbGetQuery(leakyDB,"SELECT PointIDX, ReachIDX From Points"),dbGetQuery(leakyDB,"SELECT ReachIDX, CoordinateIDX From Reaches"))
                     ),by=c("Name"="Reach")
)

writeData=data.frame(BatchIDX=thisBatchID,
                     DataTypeIDX=morph_data$DataTypeIDX,
                     PointIDX=morph_data$PointIDX,
                     SourceIDX=thisSource,
                     DateTime='',
                     QCStatusIDX=qcStatusOK,
                     Value=morph_data$value)

dbWriteTable(leakyDB,"Data",writeData,append=T)



#-----------project all as 32613 and snap all coordinates to streams-------------
toProject=dbGetQuery(leakyDB,"SELECT RowIDX, X, Y, EPSG FROM Coordinates WHERE NOT EPSG = '32613'")

apply(toProject,MARGIN=1,FUN=convertWrite)

alignPtsToStreams()

