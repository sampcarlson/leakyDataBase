library(rgrass7)
library(sp)
library(rgdal)

splitOnComma=function(inString,which){
  inString=as.character(inString)
  if(which=='x'){return(paste0("-",strsplit(inString,", ")[[1]][2]))}
  if(which=='y'){return(strsplit(inString,", ")[[1]][1])}
}

addUnitMethod=function(df,name_unit_method_list){
  df$metric=as.character(df$metric)
  df$unit=" "
  df$method=" "
  for(i in 1:length(name_unit_method_list)){
    df$metric[df$metric==name_unit_method_list[[i]]$old_name]=name_unit_method_list[[i]]$new_name
    df$unit[df$metric==name_unit_method_list[[i]]$new_name]=name_unit_method_list[[i]]$unit
    df$method[df$metric==name_unit_method_list[[i]]$new_name]=name_unit_method_list[[i]]$method
  }
  return(df)
}

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

#data, containing $X, $Y, $dateTime, $value, $QCStatusOK, $metric, $unit, $method, $areaName, $areaPath
addData=function(dataDF,batchName,batchSource,flags=defaultFlags,...){
  f=updateFlags(flags,...)
  if(all(c("areaPath","X") %in%  names(dataDF))){
    f$addAreas=T
    f$addPoints=T
    f$addMidpoint=F
    print("Adding areas and points:")
    if(!all( c("X","Y","dateTime", "value", "QCStatusOK", "metric", "unit", "method", "areaName","areaPath") %in% names(dataDF) )){
      stop(paste(paste(names(dataDF),collapse=","),"missing one or more of :X, Y, dateTime, value, QCStatusOK, metric, unit, method, areaName"))
    } else{
      dataDF=dataDF[,names(dataDF) %in% c("X","Y","dateTime", "value", "QCStatusOK", "metric", "unit", "method", "areaName", "areaPath")]
      dataDF=dataDF[complete.cases(dataDF),]
    }
  } else if("areaPath" %in% names(dataDF)){
    f$addAreas=T
    f$addPoints=F
    if(f$addMidpoint){
      print("Adding areas and midpoints:")
    }
    if(!f$addMidpoint){
      print("Adding areas:")
    }
    if(!all( c("dateTime", "value", "QCStatusOK", "metric", "unit", "method", "areaName","areaPath") %in% names(dataDF) )){
      stop(paste(paste(names(dataDF),collapse=","),"missing one or more of :dateTime, value, QCStatusOK, metric, unit, method, areaName"))
    } else{
      dataDF=dataDF[,names(dataDF) %in% c("X","Y","dateTime", "value", "QCStatusOK", "metric", "unit", "method", "areaName", "areaPath")]
      dataDF=dataDF[complete.cases(dataDF),]
    }
  } else {
    f$addPoints=T
    f$addAreas=F
    print("Adding points:")
    if(!all( c("X","Y","dateTime", "value", "QCStatusOK", "metric", "unit", "method") %in% names(dataDF) )){
      stop(paste(paste(names(dataDF),collapse=","),"missing one or more of :X, Y, dateTime, value, QCStatusOK, metric, unit, method"))
    } else{
      dataDF=dataDF[,names(dataDF) %in% c("X","Y","dateTime", "value", "QCStatusOK", "metric", "unit", "method")]
      dataDF=dataDF[complete.cases(dataDF),]
    }
  }
  
  if(f$addPoints){
    dataDF$X=as.numeric(as.character(dataDF$X))
    dataDF$Y=as.numeric(as.character(dataDF$Y))
  }
  
  if(!"QCStatusOK" %in% names(dataDF)){
    dataDF$QCStatusOK="TRUE"
  }
  
  addData_worker=function(dataDF, #single row
                          batchName,batchSource,flags=f){
    f=flags
    dataDF=data.frame(t(dataDF))
    thisDataTypeIDX=addDataType(metric=dataDF$metric,unit=dataDF$unit,method=dataDF$method)
    thisBatchIDX=addBatch(batchName,batchSource)
    if(f$addAreas){
      thisLocationIDX=addLocation(pointXYdf=dataDF,areaName=dataDF$areaName, inShpDSN=as.character(dataDF$areaPath), flags=f)
    }  else {
      thisLocationIDX=addLocation(pointXYdf=dataDF,flags=f)
    }
    writeDF=data.frame(dataTypeIDX=thisDataTypeIDX,
                       batchIDX=thisBatchIDX,
                       locationIDX=thisLocationIDX,
                       dateTime=dataDF$dateTime, 
                       value=dataDF$value, 
                       QCStatusOK = as.numeric(dataDF$QCStatusOK))
    compareNames=c("dataTypeIDX","batchIDX","locationIDX","dateTime","value","QCStatusOK")
    if(f$compareData){
      dataIDX=writeIfNew(writeDF = writeDF, table="data",
                         compareNames = compareNames,
                         idxColName = "dataIDX")
    } else {
      dataIDX=writeIfNew(writeDF = writeDF, table="data",
                         compareNames = compareNames,
                         idxColName = "dataIDX",compare=F)    
    }
    #print(paste("added datum:",dataIDX))
  }
  
  #convert epsg & write points individually
  print("reprojecting and adding data...")
  a=apply(X=dataDF,MARGIN = 1, FUN=addData_worker,batchName=batchName,batchSource=batchSource)
  
  #vectorized snap
  toSnap=dbGetQuery(leakyDB,"SELECT * FROM Points WHERE onStream = '0'")
  if(nrow(toSnap)>0){
    print("snapping points to streams...")
    writeDF=snapToStream(toSnap,flags=f)
    #drop from points, append snapped data
    dbExecute(leakyDB,"DELETE FROM Points WHERE onStream = '0'")
    
    if("X"%in%names(writeDF)){
      writeDF[names(writeDF)=="X"]=format(writeDF[names(writeDF)=="X"],10)
    }
    if("Y"%in%names(writeDF)){
      writeDF[names(writeDF)=="Y"]=format(writeDF[names(writeDF)=="Y"],10)
    }
    dbWriteTable(leakyDB,"Points",writeDF,append=T)
  }
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
    
    writeDF=data.frame(watershedID=wshedDefs$WshedName[i],areaIDX=thisAreaIDX,outPointIDX=thisPointIDX)
    
    dbWriteTable(leakyDB,name="Watersheds",value=writeDF,append=T)
  }
  return(wshedDefs$WshedName)
}

getNewIDX=function(table,idxColName){
  maxIDX=dbGetQuery(leakyDB,paste0("SELECT MAX( ",idxColName," ) FROM ",table))[[1]]
  if(is.na(maxIDX)){
    newIDX=1
  } else {
    newIDX=maxIDX+1
  }
  return(newIDX)
}

writeIfNew=function(writeDF,table,compareNames,idxColName,write=T,compare=T){    #works with 1-row writeDF only at the moment...
  # writeDF=data.frame(metric="oscar",unit="is a",method="poodle")
  # table="DataTypes"
  # compareNames=c("metric","unit","method")
  # idxColName="dataTypeIDX"
  if("X"%in%names(writeDF)){
    writeDF[names(writeDF)=="X"]=format(writeDF[names(writeDF)=="X"],10)
  }
  if("Y"%in%names(writeDF)){
    writeDF[names(writeDF)=="Y"]=format(writeDF[names(writeDF)=="Y"],10)
  }
  
  if(compare){
    returnNameVal=function(name,df){
      thisVal=df[names(df)==name][1,1]
      return(paste0(name,"='",thisVal,"'"))
    }
    whereDF=writeDF[,names(writeDF) %in% compareNames]
    
    if(length(compareNames)>1){
      where=paste0(sapply(names(whereDF),FUN=returnNameVal,df=whereDF),collapse=" AND ")
      compareIDX=dbGetQuery(leakyDB,paste0("SELECT ",idxColName," FROM ",table," WHERE ",where))[[1]]
    } 
    
    if(length(compareNames)==1){
      compareIDX=dbGetQuery(leakyDB,paste0("SELECT ",idxColName," FROM ",table," WHERE ",compareNames,"='",whereDF[[1]],"'"))[[1]]
    }
    
    
    if(length(compareIDX)==0){ #if there is no comparable entry, generate new idx, and write 
      thisIDX=getNewIDX(table=table,idxColName=idxColName)
      writeDF$tempIDX=thisIDX
      names(writeDF)[names(writeDF)=="tempIDX"]=idxColName
      if(write){
        dbWriteTable(leakyDB,table,writeDF,append=T)
      }
      
    } else { #if there is a comparable entry, use it's idx, and don't write
      thisIDX=compareIDX
      writeDF$tempIDX=thisIDX
      names(writeDF)[names(writeDF)=="tempIDX"]=idxColName
    }
    
  } else { #if no comparison w/ existing data is to be made
    thisIDX=getNewIDX(table=table,idxColName=idxColName)
    writeDF$tempIDX=thisIDX
    names(writeDF)[names(writeDF)=="tempIDX"]=idxColName
    if(write){
      dbWriteTable(leakyDB,table,writeDF,append=T)
    }
    
  }
  return(as.numeric(writeDF[names(writeDF)==idxColName][[1]]))
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
  thisAreaIDX=0
  thisPointIDX=0
  
  if(f$addAreas){
    thisIsPoint=F
    
    areaIDXs=addArea(areaName=areaName,inShpDSN=inShpDSN,flags=f)
    #areas have midpoints by default, however:
    if(f$addMidpoint & !(f$addPoints)){ # only add midpoint if no point coords are specified
      thisAreaIDX=areaIDXs$areaIDX
      thisPointIDX=areaIDXs$pointIDX
    } else {  #case for no points or midpoints
      thisAreaIDX=areaIDXs
    }
  }
  if(f$addPoints){
    thisIsPoint=T
    thisPointIDX=addPoint(X=pointXYdf$X,Y=pointXYdf$Y,flags=f)
  }
  
  writeDF=data.frame(isPoint=as.numeric(thisIsPoint),pointIDX=thisPointIDX,areaIDX=thisAreaIDX,watershedID="not assigned")
  
  #possible NULL comparison error
  locationIDX=writeIfNew(writeDF=writeDF,
                         table="Locations",
                         compareNames=c("isPoint","pointIDX","areaIDX","watershedID"),
                         idxColName="locationIDX")
  return(locationIDX)
}

addPoint=function(X,Y,flags){
  f=flags
  writeDF=data.frame(X=X,Y=Y,EPSG=f$inEPSG,onStream=as.numeric(f$onStream))
  
  
  if(f$inEPSG!=f$dbEPSG){
    writeDF=convertEPSG_df(writeDF,flags=f)
    writeDF$EPSG=f$dbEPSG
  }
  
  #this is way to slow running indivudually - vectorize across batch in addData()
  # if(as.logical(f$onStream)){
  #   writeDF=snapToStream(writeDF,flags=f)
  # }
  
  writeDF$onStream=as.integer(writeDF$onStream)
  pointIDX=writeIfNew(writeDF = writeDF, table = "Points",compareNames = c("X","Y","EPSG","onStream"),idxColName = "pointIDX")
  return(pointIDX)
}

convertEPSG_df=function(inXYdf,flags){
  f=flags
  inXYdf$tempProjectID=1:nrow(inXYdf)
  
  inp4s=paste0("+init=epsg:",f$inEPSG)
  newp4s=paste0("+init=epsg:",f$dbEPSG)
  inXYsp=SpatialPointsDataFrame(coords=data.frame(X=as.numeric(as.character(inXYdf$X)),Y=as.numeric(as.character(inXYdf$Y))),data=data.frame(inXYdf$tempProjectID))
  proj4string(inXYsp)=inp4s
  projected=spTransform(inXYsp,newp4s)
  resultXY=cbind(data.frame(projected@coords),data.frame(projected@data))
  
  inXYdf$X=NULL
  inXYdf$Y=NULL
  resultXY=left_join(inXYdf,resultXY,by=c("tempProjectID"="inXYdf.tempProjectID"))
  resultXY$tempProjectID=NULL
  return(resultXY)
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
  
  inXYdf$tempSnapID=1:nrow(inXYdf)
  
  #InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif",grassRasterName="streamRast")
  addRasterIfAbsent(grassName="streamRast",rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif")
  inXYdf$X=as.numeric(as.character(inXYdf$X))
  inXYdf$Y=as.numeric(as.character(inXYdf$Y))
  writeVECT(SDF=SpatialPointsDataFrame(coords=inXYdf[,c("X","Y")],data=data.frame(tempSnapID=inXYdf$tempSnapID)),
            "tempShape",v.in.ogr_flags = c("quiet","o","overwrite"))
  
  execGRASS("r.stream.snap",input="tempShape",stream_rast="streamRast",radius=f$streamSnapDistCells,output="tempShape_snap",flags=c("overwrite","quiet"))
  execGRASS("v.db.addtable",map="tempShape_snap",flags="quiet")
  execGRASS("v.db.join",map="tempShape_snap",column="cat",other_table="tempShape",other_column="cat",flags="quiet")
  snapped=grassTableToDF( execGRASS("v.report",map="tempShape_snap",option="coor",flags="quiet", intern=T) )
  inXYdf=plyr::rename(inXYdf,replace=c("X"="old_x","Y"="old_y"))
  
  
  result=left_join(snapped[,c("tempSnapID","x","y")],inXYdf[,!names(inXYdf) %in% c("X","Y")],by="tempSnapID")
  names(result)[names(result)=="x"]="X"
  names(result)[names(result)=="y"]="Y"
  result=result[,!(names(result) %in% c("tempSnapID","old_x","old_y"))]
  result$onStream=1
  return(result)
}

addArea=function(areaName,inShpDSN,flags){
  f=flags
  writeDF=data.frame(name=areaName)
  newAreaIDX=writeIfNew(writeDF = writeDF, table = "Areas" ,compareNames = c("name"),idxColName ="areaIDX",write=F) 
  
  if(length(dbGetQuery(leakyDB,paste0("SELECT areaIDX FROM Areas WHERE areaIDX = '",newAreaIDX,"'"))$areaIDX)==1){ # if this area is already present in db:
    
    if(f$addMidpoint & !(f$addPoints)){ #this area has a midpoint but NOT an otherwise defined point
      pointIDX=dbGetQuery(leakyDB,paste0("SELECT pointIDX FROM Locations WHERE areaIDX = '",newAreaIDX,"'"))
      newIDXs=list(areaIDX=newAreaIDX,pointIDX=pointIDX)
      return(newIDXs)
    } else { #this area has no midpoint
      return(newAreaIDX)
    }
  } else {
    if(!f$inEPSG==f$dbEPSG){
      convertEPSG_area(inShpDSN,flags=f)
      f$inEPSG=f$dbEPSG
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
    } else { #if !addMidpoint
      return( newAreaIDX )
    } 
  } 
  
}

getCoordCount=function(subFeature){
  return(nrow(subFeature@Lines[[1]]@coords))
}

createStreamSegsDF=function(){
  c=2 #number of cores to use in parallel
  library(rgeos)
  library(raster)
  library(parallel)
  library(snow)
  library(dplyr)
  
  print("load stream segs shape...")
  streamSegs=shapefile("C:/Users/Sam/Documents/spatial/r_workspaces/leakyDB/xxl_streamSegs.shp")
  #as compared to Q, the data is stored in attributes, and the attributes are stored as data
  #field 'AUTO' is unique segment ID
  
  #drop segments shorter than 2 coordinate pairs
  streamSegs=subset(streamSegs,sapply(streamSegs@lines,getCoordCount)>1)
  
  #for debug:
  #streamSegs=subset(streamSegs,c(rep(T,100),rep(F,10455)))
  print("get seg length...")
  #Function (below) from rgeos works
  streamSegs$length=gLength(streamSegs,byid=T)
  
  InitGrass_byRaster()
  
  print("write to grass...")
  writeVECT(streamSegs,vname="streamSegs",v.in.ogr_flags = "o")
  print("sample elev range...")
  execGRASS("v.rast.stats",map="streamSegs",raster="dem",column_prefix="elevRange",method="range")
  print("read & calc slope...")
  streamSegs=readVECT("streamSegs")
  streamSegs$slope=atan(streamSegs$elevRange_range/streamSegs$length) * (180/pi) #as percent
  #hist(streamSegs$slope)
  #looks reasonable?
  
  
  
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
  print("calculate midpoints...")
  segMidpoints=shapefile("C:/Users/Sam/Documents/spatial/r_workspaces/leakyDB/segPoints.shp")
  
  
  segPointsDF=data.frame(cat=segMidpoints@data$cat,X=coordinates(segMidpoints)[,1],Y=coordinates(segMidpoints)[,2])
  
  streamSegs@data=left_join(streamSegs@data,segPointsDF,by="cat")
  #streamSegs$X=sapply(streamSegs@lines,getMidpointCoords,coord="X")
  #streamSegs$Y=sapply(streamSegs@lines,getMidpointCoords,coord="Y")
  
  print("calc headings...")
  streamSegs=getHeadings(streamSegs)
  
  streamSegsDF=as.data.frame(streamSegs[,c("cat","X","Y","slope","heading_rad")])
  
  rng=function(...){
    mi=min(...,na.rm = T)
    ma=max(...,na.rm = T)
    return(ma-mi)
  }
  dem_rast=raster("C:/Users/Sam/Documents/spatial/data/dem/leakyRivers/trim/LeakyRiversDEM_rectTrim_knobFix.tif")
  # print("sample elev range around midpoints...")
  # beginCluster(n=c)
  # streamSegsDF$elevRange_25=raster::extract(x=dem_rast,
  #                                           y=streamSegsDF[,c("X","Y")],
  #                                           buffer=25,
  #                                           fun=rng)
  # endCluster()
  # 
  print("sample elevation")
  streamSegsDF$elevation=raster::extract(x=dem_rast,
                                         y=streamSegsDF[,c("X","Y")])
  
  getLateralElevRange=function(streamSegsDF,conf_range){
    
    calcLatCoords=function(segDF,thisRange){
      left_y=(sin(segDF$heading_rad+(pi/2))*thisRange)+segDF$Y
      left_x=(cos(segDF$heading_rad+(pi/2))*thisRange)+segDF$X
      right_y=(sin(segDF$heading_rad-(pi/2))*thisRange)+segDF$Y
      right_x=(cos(segDF$heading_rad-(pi/2))*thisRange)+segDF$X
      return(list(left_y=left_y,left_x=left_x,right_y=right_y,right_x=right_x))
    }
    
    latCoords=calcLatCoords(streamSegsDF,conf_range)
    #beginCluster(n=c) #not as intensive as the buffered extraction above, but still benefits from parallel
    left_elev=raster::extract(x=dem_rast,y=data.frame(X=latCoords$left_x,Y=latCoords$left_y))
    right_elev=raster::extract(x=dem_rast,y=data.frame(X=latCoords$right_x,Y=latCoords$right_y))
    #endCluster()
    latRange=mapply(rng,left_elev,right_elev,streamSegsDF$elevation)
    
    
    return(latRange)
  }
  
  getMinLateralElevRange=function(streamSegsDF,conf_range){
    
    calcLatCoords=function(segDF,thisRange){
      left_y=(sin(segDF$heading_rad+(pi/2))*thisRange)+segDF$Y
      left_x=(cos(segDF$heading_rad+(pi/2))*thisRange)+segDF$X
      right_y=(sin(segDF$heading_rad-(pi/2))*thisRange)+segDF$Y
      right_x=(cos(segDF$heading_rad-(pi/2))*thisRange)+segDF$X
      return(list(left_y=left_y,left_x=left_x,right_y=right_y,right_x=right_x))
    }
    
    latCoords=calcLatCoords(streamSegsDF,conf_range)
    #beginCluster(n=c) #not as intensive as the buffered extraction above, but still benefits from parallel
    left_elev=raster::extract(x=dem_rast,y=data.frame(X=latCoords$left_x,Y=latCoords$left_y))
    right_elev=raster::extract(x=dem_rast,y=data.frame(X=latCoords$right_x,Y=latCoords$right_y))
    #endCluster()
    latRangeRight=mapply(rng,right_elev,streamSegsDF$elevation)
    latRangeLeft=mapply(rng,left_elev,streamSegsDF$elevation)
    latRangeMin=mapply(min,latRangeRight,latRangeLeft)
    
    return(latRangeMin)
  }
  
  print("sample lat range 10...")
  streamSegsDF$latRange_10=getLateralElevRange(streamSegsDF,conf_range = 10)
  streamSegsDF$minLatRange_10=getMinLateralElevRange(streamSegsDF,conf_range = 10)
  
  print("sample lat range 25...")
  streamSegsDF$latRange_25=getLateralElevRange(streamSegsDF,conf_range = 25)
  streamSegsDF$minLatRange_25=getMinLateralElevRange(streamSegsDF,conf_range = 25)
  
  
  naMax=function(...){
    return(max(..., na.rm=T))
  }
  #removed from sample:
  #,buffer=5,fun=naMax, na.rm=T
  #beginCluster(n=c)
  print("sample UAA...")
  #add buffer to ensure stream-representitive value  !!! Nope, change to sfd in r.watershed instead
  streamSegsDF$UAA=raster::extract(x=raster("C:/Users/Sam/Documents/spatial/r_workspaces/leakyDB/flowAccum_xxl.tif"),
                                   y=streamSegsDF[,c("X","Y")])
  #endCluster()
  #beginCluster(n=c)
  print("sample SPI...")
  streamSegsDF$SPI=raster::extract(x=raster("C:/Users/Sam/Documents/spatial/r_workspaces/leakyDB/streamPower_xxl.tif"),
                                   y=streamSegsDF[,c("X","Y")])
  #endCluster()
  
  
  streamSegsDF$UAA=streamSegsDF$UAA*(9.118818^2)/(1000^2)
  
  print("write & done!")
  write.csv(streamSegsDF,"StreamSegs_slope_conf_xxl.csv")
  return("BOO!")
}

inWatershed=function(watershedIDs){
  
  allPoints=dbGetQuery(leakyDB,"SELECT Locations.LocationIDX, Locations.PointIDX, Points.X, Points.Y FROM Locations LEFT JOIN Points ON Locations.PointIDX = Points.PointIDX WHERE Locations.isPoint = '1' AND Locations.watershedID = 'not assigned'")
  writeVECT(SDF=sp::SpatialPointsDataFrame(coords=allPoints[,c("X","Y")],data=allPoints[,c("locationIDX","pointIDX")]),
            vname="allPoints",v.in.ogr_flags = c("o","overwrite","quiet"))
  
  for(watershedID in watershedIDs){  
    w_area_path=dbGetQuery(leakyDB,paste0("SELECT fileName FROM Areas LEFT JOIN Watersheds ON Areas.areaIDX = Watersheds.areaIDX WHERE Watersheds.watershedID = '",watershedID,"'"))$fileName
    execGRASS("v.in.ogr",input=w_area_path,output="watershedArea",flags=c("overwrite","quiet"))
    execGRASS("v.select",ainput="allPoints",binput="watershedArea",output="ptsInWshed",operator="overlap",flags=c("overwrite","quiet"))
    locIdxInWshed=grassTableToDF( execGRASS("v.db.select",map="ptsInWshed",intern = T) )$locationIDX
    if(length(locIdxInWshed)>0){
      dbExecute(leakyDB,paste0("UPDATE Locations SET watershedID = '",watershedID,"' WHERE locationIDX IN (",paste(locIdxInWshed,collapse = ", "),")"))
    }
  }
}

characterizeAreas=function(areasBatchName,addDTs,newBatchName){
  #add new data types for mean data
  oldDTs=dbGetQuery(leakyDB,paste0("SELECT * FROM DataTypes WHERE DataTypes.dataTypeIDX IN (",paste(addDTs,collapse=", "),")"))
  newDTs=data.frame(oldIDX=oldDTs$dataTypeIDX)
  newDTs$metric=paste0("mean_",oldDTs$metric)
  newDTs$unit=oldDTs$unit
  newDTs$method=paste("mean of dataTypeIDX",newDTs$oldIDX)
  newDTs$newIDX=0
  #this is stupid, but oh well...
  for(i in 1:nrow(newDTs)){
    newDTs$newIDX[i]=writeIfNew(newDTs[i,c("metric","unit","method")],"DataTypes",compareNames = c("metric","unit","method"),idxColName="dataTypeIDX")
  }
  
  #add batch
  batchIDX=addBatch(newBatchName,"characterizeAreas()")
  
  aggMeanFun=function(x){
    if(is.numeric(x)){
      return(mean(x,na.rm=T))
    } else {
      return (x[1])
    }
  }
  
  areaInfo=dbGetQuery(leakyDB,paste0("SELECT DISTINCT Areas.fileName, Locations.LocationIDX FROM Areas LEFT JOIN Locations ON Areas.areaIDX = Locations.areaIDX
                                      LEFT JOIN Data ON Locations.LocationIDX = Data.LocationIDX
                                      LEFT JOIN Batches ON Data.batchIDX = Batches.batchIDX
                                      WHERE Batches.batchName = '",areasBatchName,"'"))
  
  allPoints=dbGetQuery(leakyDB,"SELECT Locations.LocationIDX, Locations.PointIDX, Points.X, Points.Y FROM Locations LEFT JOIN Points ON Locations.PointIDX = Points.PointIDX")
  #not all areas have points, however, all area characteristics have been added to regularly spaced segment points.
  #drop where pointIDX=0 or X, Y are absent
  allPoints$pointIDX[allPoints$pointIDX==0]=NA
  allPoints=allPoints[complete.cases(allPoints),]
  allPoints$X=as.numeric(as.character(allPoints$X))
  allPoints$Y=as.numeric(as.character(allPoints$Y))
  
  writeVECT(SDF=sp::SpatialPointsDataFrame(coords=allPoints[,c("X","Y")],data=allPoints[,c("locationIDX","pointIDX")]),
            vname="allPoints",v.in.ogr_flags = c("o","overwrite","quiet"))
  
  for(areaPath in areaInfo$fileName){
    thisLocation=areaInfo$locationIDX[areaInfo$fileName==areaPath]
    execGRASS("v.in.ogr",input=areaPath,output="thisArea",flags=c("overwrite","quiet"))
    execGRASS("v.select",ainput="allPoints",binput="thisArea",output="ptsInArea",operator="overlap",flags=c("overwrite","quiet"))
    locIDXs=grassTableToDF( execGRASS("v.db.select",map="ptsInArea",intern = T) )$locationIDX
    
    locData=dbGetQuery(leakyDB,paste0("SELECT * FROM Data WHERE locationIDX IN (",paste(locIDXs,collapse=", "),") AND DataTypeIDX IN (",paste(addDTs,collapse=", "),")"))
    
    if(nrow(locData)>1){
      locData=plyr::rename(locData,replace=c("dataTypeIDX"="oldDataTypeIDX","dataIDX"="oldDataIDX"))
      #aggregate
      locData=aggregate(locData,by=list(dtIDX=locData$oldDataTypeIDX),FUN=aggMeanFun)
      locData$locationIDX=thisLocation
      #join to new data types
      locData=left_join(locData,newDTs[,c("oldIDX","newIDX")],by=c("oldDataTypeIDX"="oldIDX"))
      locData$dataTypeIDX=locData$newIDX
      
      #write to DB
      locData$dataIDX=seq(from=getNewIDX("Data","dataIDX"),by=1,length.out=length(locData$dtIDX))
      newData=locData[,c("dataIDX","dataTypeIDX","locationIDX","dateTime","value","QCStatusOK")]
      newData$batchIDX=batchIDX
      #this is stupid, but oh well...
      for(i in 1:nrow(newData)){
        writeIfNew(newData[i,],"Data",compareNames = c("locationIDX"),idxColName="dataIDX",compare = F)
      }
    }
  }
}

characterizePointsByAreas=function(pointsBatch,dataTypesToAdd){
  batchIDX=addBatch("area characteristics","characterizePointsByAreas()")
  
  
  #add all points to GRASS
  locIDXs=dbGetQuery(leakyDB, paste0("SELECT DISTINCT Data.locationIDX FROM Data WHERE Data.batchIDX = '",pointsBatch,"'"))$locationIDX
  pointDF=dbGetQuery(leakyDB,paste0("SELECT Locations.locationIDX, Points.X, Points.Y FROM Locations LEFT JOIN Points ON Locations.pointIDX = Points.PointIDX
                                    WHERE Locations.locationIDX IN (",paste(locIDXs,collapse=", "),")"))
  
  writeVECT(SDF=SpatialPointsDataFrame(coords=pointDF[,c("X","Y")],data=data.frame(locationIDX = pointDF$locationIDX)),
            vname="allPoints",v.in.ogr_flags = c("o","overwrite","quiet"))
  
  #get DF of all areas
  areasDF=dbGetQuery(leakyDB,paste0("SELECT DISTINCT Areas.areaIDX, Areas.fileName FROM Areas LEFT JOIN Locations ON Areas.areaIDX = Locations.areaIDX 
                                    LEFT JOIN Data ON Locations.locationIDX = Data.locationIDX WHERE Data.dataTypeIDX IN (",paste(dataTypesToAdd,collapse=", "),")"))
  
  #iterate through areas, adding each area, identifying points within the area, and adding data to points by Data.locationIDX
  for(i in 1:nrow(areasDF)){
    thisAreaIDX=areasDF$areaIDX[i]
    thisAreaPath=areasDF$fileName[i]
    execGRASS("v.in.ogr",input=thisAreaPath,output="thisArea",flags=c("overwrite","quiet"))
    execGRASS("v.select",ainput="allPoints",binput="thisArea",output="ptsInArea",operator="overlap",flags=c("overwrite","quiet"))
    thisLocIDXs=grassTableToDF( execGRASS("v.db.select",map="ptsInArea",intern = T) )$locationIDX
    
    if(length(thisLocIDXs>0)){
      #get relevant data associated with area
      thisAreaData=dbGetQuery(leakyDB,paste0("SELECT Data.dataTypeIDX, Data.value, Data.dateTime, Data.QCStatusOK FROM DATA
                                LEFT JOIN Locations ON Data.locationIDX = Locations.LocationIDX
                                WHERE Locations.areaIDX = '",thisAreaIDX,"'"))
      thisAreaData=thisAreaData[thisAreaData$dataTypeIDX %in% dataTypesToAdd,]
      thisAreaData$batchIDX=batchIDX
      if(nrow(thisAreaData)>0){
        
        for(thisLocIDX in thisLocIDXs){
          thisAreaData$locationIDX=thisLocIDX
          for(j in 1:nrow(thisAreaData)){
            dataID=writeIfNew(thisAreaData[j,],"Data",compareNames = c("locationIDX"),idxColName="dataIDX",compare = F)
          }
        }
        
      }
    }
  }
}
