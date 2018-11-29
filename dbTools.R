library(rgrass7)
library(sp)
library(rgdal)

delete_data=function(db,table){
  dbExecute(db,paste0("DELETE FROM ",table))
}

addData=function(dataDF,                                      #data
                 batchName,batchSource,                       #batch
                 metric,unit,method,                          #data type
                 pointXYdf=NULL,inShpDSN=NULL, areaName=NULL, #location
                 inEPSG=targetEPSG,snapToStream=T,targetEPSG=targetEPSG, addMidpoint=addMidpoint){ #defaults
  dataTypeIDX=addDataType(metric,unit,method)
  batchIDX=addBatch(batchName,batchSource)
  locationIDX
  
}

addWatershedDefinitions=function(wshedDefs){
  #InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif",grassRasterName="streamRast")
  addRasterIfAbsent(grassName="streamRast",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif")
  execGRASS("r.in.gdal")
  
  writeVECT(SDF=SpatialPointsDataFrame(coords=wshedDefs[,c("X","Y")],data=wshedDefs[,c("WshedName","id")]),"wshedOutPoints",v.in.ogr_flags=c("o","quiet"))
  execGRASS("r.stream.snap",input="wshedOutPoints",stream_rast="streamRast",radius=10,output="wshedOutPoints_snap",flags="quiet")
  execGRASS("v.db.addtable",map="wshedOutPoints_snap",flags="quiet")
  execGRASS("v.db.join",map="wshedOutPoints_snap",column="cat",other_table="wshedOutPoints",other_column="cat",flags="quiet")
  wshedDefs=grassTableToDF( execGRASS("v.report",map="wshedOutPoints_snap",option="coor",flags="quiet", intern=T) )
  execGRASS("r.in.gdal",input="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/flowDir_xxl.tif",output="flowDir",flags="quiet")
  
  for(i in 1:nrow(wshedDefs)){
    #calculate and write area:
    execGRASS("r.water.outlet", input="flowDir",output="above_point_temp",coordinates=c(wshedDefs$x[i],wshedDefs$y[i]), flags=c("overwrite","quiet"))
    execGRASS("r.to.vect",input="above_point_temp",output="dbWriteMe",type="area",flags=c("overwrite","quiet"))
    execGRASS("v.out.ogr",input="dbWriteMe",output=paste0(dbShapePath,"tempShape.shp"),format="ESRI_Shapefile",flags=c("overwrite","quiet"))
    
    thisAreaIDX=addArea(EPSG=32613)
    
    #calculate and write point:
    thisPointIDX=addPoint(X=wshedDefs$x[i],Y=wshedDefs$y[i])
    
    #write to watersheds table:
    #writeIfNew(writeDF=writeDF,table="Watersheds",compareNames = c("areaIDX","outPointIDX"),idxColName = "watershedID")
    #ignore duplicate issues here by writing exactly once - these are single-purpose points
    
    dbWriteTable(leakyDB,"Watersheds",data.frame(watershedID=wshedDefs$WshedName[i],areaIDX=thisAreaIDX,outPointIDX=thisPointIDX),append=T)
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
  
  where=paste0(sapply(names(writeDF),FUN=returnNameVal,df=writeDF),collapse=" AND ")
  
  dataTypeIDX=dbGetQuery(leakyDB,paste0("SELECT ",idxColName," FROM ",table," WHERE ",where))[[1]]
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

addLocation=function(pointXYdf=NULL,inShpDSN=NULL,areaName=NULL,inEPSG=targetEPSG,snapToStream=T,targetEPSG=targetEPSG, addMidpoint = addMidpoint){
  if(!is.null(areaName)){
    areaIDXs=addArea(areaName,inShpDSN,inEPSG=targetEPSG,targetEPSG=targetEPSG,dbShapePath=dbShapePath,dbShapeName="leakyArea_",addMidpoint = addMidpoint)
    if(addMidpoint){
      areaIDX=areaIDXs$areaIDX
      pointIDX=areaIDXs$pointIDX
    } else {
      areaIDX=areaIDXs
    }
  } else {
    areaIDX=NULL
    pointIDX=addPoint(X=pointXYdf$X,Y=pointXYdf$Y,inEPSG=inEPSG,snapToStream = snapToStream, targetEPSG = targetEPSG)
  }
  #areas have midpoints by default
  writeDF=data.frame(isPoint=T,pointIDX=pointIDX,areaIDX=NULL,watershedID="not assigned")
  
  #possible NULL comparison error
  locationIDX=writeIfNew(writeDF=writeDF,table="Locations",compareNames=c("isPoint","pointIDX","areaIDX","watershedID"),idxColName="locationIDX")
  return(locationIDX)
}

addPoint=function(X,Y,inEPSG=targetEPSG,snapToStream=T,targetEPSG=targetEPSG){
  writeDF=data.frame(X=X,Y=Y,EPSG=EPSG,onStream=onStream)
  
  if(snapToStream){
    writeDF=snapToStream(writeDF, inEPSG = inEPSG, targetEPSG=targetEPSG)
  } else {
    writeDF=convertEPSG_df(writeDF,inEPSG=inEPSG, targetEPSG=targetEPSG)
  }
  
  pointIDX=writeIfNew(writeDF = writeDF, table = "Points",compareNames = c("X","Y","EPSG","onStream"),idxColName = "pointIDX")
  return(pointIDX)
}

convertEPSG_df=function(inXYdf, inEPSG=4326, targetEPSG=targetEPSG){
  
  sp::coordinates(inXYdf)=c("X","Y")
  inp4s=paste0("+init=epsg:",inEPSG)
  newp4s=paste0("+init=epsg:",targetEPSG)
  
  proj4string(projectMe)=inp4s
  projected=spTransform(inXYdf,newp4s)
  result=cbind(projected@data,projected@coords)
  return(result)
}

convertEPSG_area=function(inShpDSN = inShpDSN,targetEPSG=targetEPSG){
  tempShape=rgdal::readOGR(dsn=inShpDSN)
  newp4s=paste0("+init=epsg:",targetEPSG)
  tempShape_projected=spTransform(tempShape,newp4s)
  rgdal::writeOGR(tempShape_projected,inShpDSN,driver="ESRI Shapefile",layer="area",overwrite_layer = TRUE)
}

snapToStream=function(inXYdf, inEPSG=targetEPSG, targetEPSG=targetEPSG){
  if(inEPSG!=targetEPSG){
    inXYdf=convertEPSG_df(inXYdf,inEPSG=inEPSG, targetEPSG=targetEPSG)
  }
  
  inXYdf$tempSnapID=1:nrow(inXYdf)
  
  #InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif",grassRasterName="streamRast")
  addRasterIfAbsent(grassName="streamRast",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif")
  writeVECT(SDF=SpatialPointsDataFrame(coords=inXYdf[,c("X","Y")],data=data.frame(tempSnapID=inXYdf$tempSnapID)),
            "tempShape",v.in.ogr_flags = c("o","overwrite"))
  
  execGRASS("r.stream.snap",input="tempShape",stream_rast="streamRast",radius=20,output="tempShape_snap",flags=c("overwrite","quiet"))
  execGRASS("v.db.addtable",map="tempShape_snap",flags="quiet")
  execGRASS("v.db.join",map="tempShape_snap",column="cat",other_table="tempShape",other_column="cat",flags="quiet")
  snapped=grassTableToDF( execGRASS("v.report",map="tempShape_snap",option="coor",flags="quiet", intern=T) )
  result=left_join(snapped[,c("tempSnapID","x","y")],inXYdf[,!names(inXYdf) %in% c("X","Y")],by="tempSnapID")
  names(result)[names(result)=="x"]="X"
  names(result)[names(result)=="y"]="Y"
  return(result)
}

addArea=function(areaName, inShpDSN=inShpDSN,inEPSG=targetEPSG,targetEPSG=targetEPSG, dbShapePath=dbShapePath, dbShapeName=dbShapeName, addMidpoint=addMidpoint){
  writeDF=data.frame(name=areaName)
  newAreaIDX=writeIfNew(writeDF = writeDF, table = "Areas" ,compareNames = c("name"),idxColName ="areaIDX",write=F) 
  
  if(inEPSG!=targetEPSG){
    convertEPSG_area( inShpDSN = inShpDSN, targetEPSG = targetEPSG)
  }
  tempShape=rgdal::readOGR(dsn=inShpDSN)
  
  shpName=paste0(dbShapeName,newAreaIDX,".shp")
  fullPathName=paste0(dbShapePath,shpName)
  writeDF=data.frame(areaIDX=newAreaIDX,name=areaName,fileName=fullPathName,EPSG=EPSG)
  
  newAreaIDX=writeIfNew(writeDF = writeDF, table = "Areas" ,compareNames = c("name","fileName","EPSG"),idxColName ="areaIDX") 
  
  rgdal::writeOGR(tempShape,fullPathName,driver="ESRI Shapefile",layer="area",overwrite_layer = TRUE)
  
  if(addMidpoint){
    execGRASS("v.in.ogr", input=fullPathName,output="tempShape",flags=c("quiet","overwrite"))
    addRasterIfAbsent(grassName="streamRast",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif")
    #addRasterIfAbsent(grassName="UAA",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/flowAccum_xxl.tif")
    execGRASS("v.centroids",input="tempShape",output="tempShapeCentroid",option="add",flags=c("overwrite","quiet"))
    
    #wtf
    xy = execGRASS("v.out.ascii", input="tempShapeCentroid", type="centroid",intern=T) 
    xy=strsplit(xy,split="|",fixed=T)
    X=xy[[1]][1]
    Y=xy[[1]][2]
    
    pointIDX=addPoint(X=X, Y=Y)
    newIDXs=list(areaIDX=newAreaIDX,pointIDX=pointIDX)
    return( newIDXs )
  } else {
    return( newAreaIDX )
  }
  
}

inWatershed=function(defPointIDX=NULL,defXY=NULL){}