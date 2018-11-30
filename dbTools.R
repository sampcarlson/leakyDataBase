library(rgrass7)
library(sp)
library(rgdal)

updateFlags=function(flags,...){
  l = list(...)
  for(n in names(l)){
    if(n %in% names(flags)){
      flags[names(flags)==n]=l[names(l)==n]
    } else print(paste("Unknown flag:",n))
  }
  return(flags)
}

delete_data=function(db,table){
  dbExecute(db,paste0("DELETE FROM ",table))
}

#data, containing $X, $Y, $dateTime, $value, $QCStatusOK, $metric, $unit, $method
addData_points=function(dataDF,batchName,batchSource,flags=defaultFlags,...){
  f=updateFlags(flags,...)
  if(!"QCStatusOK" %in% names(dataDF)){
    dataDF$QCStatusOK="TRUE"
  }
  addData_points_worker=function(dataDF, #single row
                                 batchName,batchSource){
    dataTypeIDX=addDataType(metric,unit,method)
    batchIDX=addBatch(batchName,batchSource)
    locationIDX=addLocation(pointXYdf=dataDF,flags=f)
    
    writeDF=data.frame(dataTypeIDX=dataTypeIDX,batchIDX=batchIDX,locationIDX=locationIDX,
                       dateTime=dataDF$dateTime, value=dataDF$value, QCStatusOK = data$QCStatusOK)
    dataIDX=writeIfNew(writeDF = writeDF, table="data",
                       compareNames=c("dataTypeIDX","batchIDX","locationIDX","dateTime","value","QCStatusOK"),
                       idxColName = "dataIDX")
  }
  
  apply(X=dataDF,MARGIN = 1, FUN=addData_points_worker)
}

addWatershedDefinitions=function(wshedDefs,flags=defaultFlags,...){
  f=updateFlags(flags,...)
  #InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif",grassRasterName="streamRast")
  addRasterIfAbsent(grassName="streamRast",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif")
  
  writeVECT(SDF=SpatialPointsDataFrame(coords=wshedDefs[,c("X","Y")],data=wshedDefs[,c("WshedName","id")]),"wshedOutPoints",v.in.ogr_flags=c("overwrite","o","quiet"))
  execGRASS("r.stream.snap",input="wshedOutPoints",stream_rast="streamRast",radius=10,output="wshedOutPoints_snap",flags=c("overwrite","quiet"))
  execGRASS("v.db.addtable",map="wshedOutPoints_snap",flags="quiet")
  execGRASS("v.db.join",map="wshedOutPoints_snap",column="cat",other_table="wshedOutPoints",other_column="cat",flags=c("quiet"))
  wshedDefs=grassTableToDF( execGRASS("v.report",map="wshedOutPoints_snap",option="coor",flags="quiet", intern=T) )
  addRasterIfAbsent(grassName="flowDir",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/flowDir_xxl.tif")
  
  for(i in 1:nrow(wshedDefs)){
    thisName=wshedDefs$WshedName[i]
    #calculate and write area:
    execGRASS("r.water.outlet", input="flowDir",output="above_point_temp",coordinates=c(as.numeric(wshedDefs$x[i]),as.numeric(wshedDefs$y[i])), flags=c("overwrite","quiet"))
    execGRASS("r.to.vect",input="above_point_temp",output="dbWriteMe",type="area",flags=c("overwrite","quiet"))
    execGRASS("v.out.ogr",input="dbWriteMe",output=paste0(f$dbShapePath,"tempShape.shp"),format="ESRI_Shapefile",flags=c("overwrite","quiet"))
    thisAreaIDX=addArea(areaName=thisName,inShpDSN=paste0(f$dbShapePath,"tempShape.shp"),flags=f)
    
    #calculate and write point:
    thisPointIDX=addPoint(X=wshedDefs$x[i],Y=wshedDefs$y[i],flags=f)
    
    #write to watersheds table:
    #writeIfNew(writeDF=writeDF,table="Watersheds",compareNames = c("areaIDX","outPointIDX"),idxColName = "watershedID")
    #ignore duplicate issues here by writing exactly once - these are single-purpose points
    
    writeDF=data.frame(watershedID=wshedDefs$WshedName[i],areaIDX=thisAreaIDX,outPointIDX=thisPointIDX)
    
    dbWriteTable(leakyDB,name="Watersheds",value=writeDF,append=T)
  }
  return(wshedDefs$WshedName)
}

getNewIDX=function(table,idxColName){
  maxIDX=dbGetQuery(leakyDB,paste0("SELECT MAX( ",idxColName," ) FROM ",table))[[1]]
  if(is.na(maxIDX)){
    newIDX=0
  } else {
    newIDX=maxIDX+1
  }
}

writeIfNew=function(writeDF,table,compareNames,idxColName,write=T){    #works with 1-row writeDF only at the moment...
  # writeDF=data.frame(metric="oscar",unit="is a",method="poodle")
  # table="DataTypes"
  # compareNames=c("metric","unit","method")
  # idxColName="dataTypeIDX"
  
  returnNameVal=function(name,df){
    thisVal=df[names(df)==name][1,1]
    return(paste0(name,"='",thisVal,"'"))
  }
  whereDF=writeDF[,names(writeDF) %in% compareNames]
  
  if(length(compareNames)>1){
    where=paste0(sapply(names(whereDF),FUN=returnNameVal,df=whereDF),collapse=" AND ")
    dataTypeIDX=dbGetQuery(leakyDB,paste0("SELECT ",idxColName," FROM ",table," WHERE ",where))[[1]]
  } 
  
  if(length(compareNames)==1){
    dataTypeIDX=dbGetQuery(leakyDB,paste0("SELECT ",idxColName," FROM ",table," WHERE ",compareNames,"='",whereDF[[1]],"'"))[[1]]
  }
  
  if(length(dataTypeIDX)==0){
    newIDX=getNewIDX(table=table,idxColName=idxColName)
    writeDF$tempIDX=newIDX
    names(writeDF)[names(writeDF)=="tempIDX"]=idxColName
    if(write){
      dbWriteTable(leakyDB,table,writeDF,append=T)
    }
  } else {
    writeDF$tempIDX=dataTypeIDX
    names(writeDF)[names(writeDF)=="tempIDX"]=idxColName
    
  }
  return(writeDF[names(writeDF)==idxColName][[1]])
}

addDataType=function(metric, unit, method){
  writeDF=data.frame(metric=metric, unit=unit, method=method)
  dataTypeIDX=writeIfNew(writeDF=writeDF, table="DataTypes",compareNames=c("metric","unit","method"),idxColName = "dataTypeIDX")
  return(dataTypeIDX)
}

addBatch=function(batchName,batchSource){
  writeDF=data.frame(batchName=batchName,importDateTime=date(),source=batchSource)
  batchIDX=writeIfNew(writeDF=writeDF,table="Batches",compareNames=c("batchName","source"),idxColName="batchIDX")
  return(batchIDX)
}

addLocation=function(pointXYdf=NULL,inShpDSN=NULL,areaName=NULL,flags){
  f=flags
  if(!is.null(areaName)){
    areaIDXs=addArea(areaName,inShpDSN,flags=f)
    if(f$addMidpoint){
      areaIDX=areaIDXs$areaIDX
      pointIDX=areaIDXs$pointIDX
    } else {
      areaIDX=areaIDXs
    }
  } else {
    areaIDX=NULL
    pointIDX=addPoint(X=pointXYdf$X,Y=pointXYdf$Y,flags=f)
  }
  #areas have midpoints by default
  writeDF=data.frame(isPoint=T,pointIDX=pointIDX,areaIDX=NULL,watershedID="not assigned")
  
  #possible NULL comparison error
  locationIDX=writeIfNew(writeDF=writeDF,table="Locations",compareNames=c("isPoint","pointIDX","areaIDX","watershedID"),idxColName="locationIDX")
  return(locationIDX)
}

addPoint=function(X,Y,flags){
  f=flags
  writeDF=data.frame(X=X,Y=Y,EPSG=f$inEPSG,onStream=f$onStream)
  
  if(f$inEPSG!=f$dbEPSG){
    writeDF=convertEPSG_df(writeDF,inEPSG=f$inEPSG,flags=f)
  }
  
  if(f$onStream){
    writeDF=snapToStream(writeDF,flags=f)
  }
  
  pointIDX=writeIfNew(writeDF = writeDF, table = "Points",compareNames = c("X","Y","EPSG","onStream"),idxColName = "pointIDX")
  return(pointIDX)
}

convertEPSG_df=function(inXYdf,flags){
  f=flags
  sp::coordinates(inXYdf)=c("X","Y")
  inp4s=paste0("+init=epsg:",f$inEPSG)
  newp4s=paste0("+init=epsg:",f$dbEPSG)
  
  proj4string(inXYdf)=inp4s
  projected=spTransform(inXYdf,newp4s)
  result=cbind(projected@data,projected@coords)
  return(result)
}

convertEPSG_area=function(inShpDSN,flags){
  f=flags
  tempShape=rgdal::readOGR(dsn=inShpDSN)
  newp4s=paste0("+init=epsg:",f$dbEPSG)
  tempShape_projected=spTransform(tempShape,newp4s)
  rgdal::writeOGR(tempShape_projected,inShpDSN,driver="ESRI Shapefile",layer="area",overwrite_layer = TRUE)
}

snapToStream=function(inXYdf,flags){
  f=flags
  if(f$inEPSG!=f$dbEPSG){
    inXYdf=convertEPSG_df(inXYdf,flags=f)
  }
  
  inXYdf$tempSnapID=1:nrow(inXYdf)
  
  #InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif",grassRasterName="streamRast")
  addRasterIfAbsent(grassName="streamRast",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif")
  inXYdf$X=as.numeric(as.character(inXYdf$X))
  inXYdf$Y=as.numeric(as.character(inXYdf$Y))
  writeVECT(SDF=SpatialPointsDataFrame(coords=inXYdf[,c("X","Y")],data=data.frame(tempSnapID=inXYdf$tempSnapID)),
            "tempShape",v.in.ogr_flags = c("quiet","o","overwrite"))
  
  execGRASS("r.stream.snap",input="tempShape",stream_rast="streamRast",radius=20,output="tempShape_snap",flags=c("overwrite","quiet"))
  execGRASS("v.db.addtable",map="tempShape_snap",flags="quiet")
  execGRASS("v.db.join",map="tempShape_snap",column="cat",other_table="tempShape",other_column="cat",flags="quiet")
  snapped=grassTableToDF( execGRASS("v.report",map="tempShape_snap",option="coor",flags="quiet", intern=T) )
  result=left_join(snapped[,c("tempSnapID","x","y")],inXYdf[,!names(inXYdf) %in% c("X","Y")],by="tempSnapID")
  names(result)[names(result)=="x"]="X"
  names(result)[names(result)=="y"]="Y"
  result=result[,names(result) != "tempSnapID"]
  return(result)
}

addArea=function(areaName,inShpDSN,flags){
  f=flags
  writeDF=data.frame(name=areaName)
  newAreaIDX=writeIfNew(writeDF = writeDF, table = "Areas" ,compareNames = c("name"),idxColName ="areaIDX",write=F) 
  
  if(!f$inEPSG==f$dbEPSG){
    convertEPSG_area(inShpDSN,flags=f)
  }
  tempShape=rgdal::readOGR(dsn=inShpDSN)
  
  shpName=paste0(f$dbShapeName,newAreaIDX,".shp")
  fullPathName=paste0(f$dbShapePath,shpName)
  writeDF=data.frame(areaIDX=newAreaIDX,name=areaName,fileName=fullPathName,EPSG=f$dbEPSG)
  
  newAreaIDX=writeIfNew(writeDF = writeDF, table = "Areas" ,compareNames = c("name","fileName","EPSG"),idxColName ="areaIDX") 
  
  rgdal::writeOGR(tempShape,fullPathName,driver="ESRI Shapefile",layer="area",overwrite_layer = TRUE)
  if(f$addMidpoint){
    execGRASS("v.in.ogr", input=fullPathName,output="tempShape",flags=c("quiet","overwrite"))
    addRasterIfAbsent(grassName="streamRast",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif")
    #addRasterIfAbsent(grassName="UAA",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/flowAccum_xxl.tif")
    execGRASS("v.centroids",input="tempShape",output="tempShapeCentroid",option="add",flags=c("overwrite","quiet"))
    
    #wtf
    xy = execGRASS("v.out.ascii", input="tempShapeCentroid", type="centroid",intern=T) 
    xy=strsplit(xy,split="|",fixed=T)
    X=xy[[1]][1]
    Y=xy[[1]][2]
    
    pointIDX=addPoint(X=X, Y=Y,flags=f)
    newIDXs=list(areaIDX=newAreaIDX,pointIDX=pointIDX)
    return( newIDXs )
  } else {
    return( newAreaIDX )
  }
  
}

inWatershed=function(defPointIDX=NULL,defXY=NULL,flags,...){}