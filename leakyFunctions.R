delete_data=function(db,table){
  dbExecute(db,paste0("DELETE FROM ",table))
}


calcDepPct=function(df){
  rowDepPct=function(r){
    if(r["Depositional area units"]=="m2"){
      #m2 measures represent 5 m upstream and downstream of sampling point
      DepAreaPct=100*as.numeric(r["Depositional area"])/(10*as.numeric(r["Wetted width"]))
    } else if(r["Depositional area units"]=="%"){
      DepAreaPct=as.numeric(r["Depositional area"])
    } else DepAreaPct=NA
    return(DepAreaPct)
  }
  df$DepArea_pct=apply(rawWidth,1,rowDepPct)
  return(df)
}


writeIfNew=function(db,tableName,df,compareNames,IDXName){
  require(RSQLite)
  if(!IDXName%in%names(df)){
    df[IDXName]=0
  }
  namesInOrder=dbListFields(db,tableName)
  df=df[,namesInOrder]
  for(r in 1:nrow(df)){
    if(r/1000==round(r/1000)){print(paste(r,"records added..."))}
    dbData=dbGetQuery(db,paste("SELECT",paste(compareNames,collapse=", "),"FROM",tableName))
    if(nrow(rbind(dbData,df[r,][compareNames]))==nrow(base::unique(rbind(dbData,df[r,][compareNames]))))
    {
      df[r,IDXName]=dbGetQuery(db,paste0("SELECT MAX(",IDXName,") FROM ",tableName))+1
      writeMe=df[r,]
      dbWriteTable(db,tableName,writeMe,append=T)
    } else {
      print(df[r,][compareNames])
      print(paste("duplicate data in",tableName))
    }
  }
  
}
#example:
#writeIfNew(db=leakyDB,tableName="CoordinateTypes",df=data.frame(Description=c('Midpoint','George','Larry')),compareNames = "Description", IDXName = "CoordinateTypeIDX")
#dbGetQuery(leakyDB,"Select * from CoordinateTypes")

addBatch=function(batchName,db=leakyDB){
  writeIfNew(leakyDB,'Batches',data.frame(Name=batchName),'Name','BatchIDX')
  return(as.numeric(dbGetQuery(leakyDB,paste0("SELECT BatchIDX FROM Batches WHERE Name = '",batchName,"'"))))
}

getCoordCount=function(subFeature){
  return(nrow(subFeature@Lines[[1]]@coords))
}

getEndpoints=function(multiLineFeature,dem=raster("~/R Projects/mapC/Spatial_Inputs/BigDemWGS84.tif")){
  endpointsHelper=function(segment,segID,dem){
    numCoords=nrow(segment@Lines[[1]]@coords)
    pointOne=cbind(segment@Lines[[1]]@coords[1,1],segment@Lines[[1]]@coords[1,2])
    pointTwo=cbind(segment@Lines[[1]]@coords[numCoords,1],segment@Lines[[1]]@coords[numCoords,2])
    if(raster::extract(dem,pointOne)>raster::extract(dem,pointTwo)){
      upPoint_x=pointOne[1]
      upPoint_y=pointOne[2]
      downPoint_x=pointTwo[1]
      downPoint_y=pointTwo[2]
    }else{
      downPoint_x=pointOne[1]
      downPoint_y=pointOne[2]
      upPoint_x=pointTwo[1]
      upPoint_y=pointTwo[2]
    }
    return(c(segID=segID,upPoint_x=upPoint_x,upPoint_y=upPoint_y,downPoint_x=downPoint_x,downPoint_y=downPoint_y))
  }
  
  #return(sapply(multiLineFeature@lines,endpointsHelper,dem=dem))
  
  return(mapply(endpointsHelper,multiLineFeature@lines,multiLineFeature$AUTO,MoreArgs=list(dem=dem),SIMPLIFY = T))
}

convertByEpsg=function(inDF, outEPSG=32613){
  xy=unlist(inDF[c("X","Y")])
  inEPSG=inDF['EPSG']
  tempPoint=st_sfc(st_point(x=xy,dim="XY"),crs=inEPSG)
  transPoint=st_transform(tempPoint,outEPSG)
  return(st_coordinates(transPoint))
}

convertWrite=function(inDF,outEPSG=32613){
  rowIdx=inDF['RowIDX']
  xy=convertByEpsg(inDF)
  result=dbSendStatement(leakyDB,paste0("UPDATE Coordinates SET X = '",xy[1],"', Y = '",xy[2],"', EPSG = '",outEPSG,"' WHERE RowIDX = '",rowIdx,"'"))
  t=dbClearResult(result)
}

getPtsConfinement=function(shpNet,networkID,interval){
  #drop segments shorter than 2 coordinate pairs
  shpNet=subset(shpNet,sapply(shpNet@lines,getCoordCount)>1)
  
  #shpNet=addLengths(shpNet) 
  #Function (below) from rgeos works 
  shpNet$length=gLength(shpNet,byid=T)
  
  rng=function(...){
    mi=min(...)
    ma=max(...)
    return(ma-mi)
  }
  shpPts=spsample(x=shpNet,n=sum(shpNet$length)/interval,type="regular")
  shpDF=data.frame(shpPts)
  names(shpDF)=c("X","Y")
  shpDF$value=extract(x=raster("C:/Users/Sam/Desktop/spatial/QgisEnvironment/Inputs_and_scripts/allDemRaw/all4_wgs84_13n.tif"),y=shpPts,buffer=25,fun=rng)
  shpDF$NetworkIDX=networkID
  shpDF$variable="elevRange25"
  return(shpDF)
}

alignPtsToStreams=function(db_source=leakyDB,allowed_radius=100,flowAcc_threshold=20000){
  allCoords=dbGetQuery(db_source,"SELECT CoordinateIDX, X, Y FROM Coordinates")
  allCoords$X=as.numeric(allCoords$X)
  allCoords$X=as.numeric(allCoords$X)
  allCoords[allCoords==0]=NA
  allCoords=allCoords[complete.cases(allCoords),]
  allCoords_sdf=SpatialPointsDataFrame(coords=allCoords[,c("X","Y")],data=data.frame(CoordinateIDX=allCoords$CoordinateIDX,RowNumber=1:nrow(allCoords)))
  
  InitGrass_byRaster(rasterPath="C:/Users/Sam/Desktop/spatial/QgisEnvironment/Inputs_and_scripts/allDemRaw/all4_wgs84_13n.tif")
  execGRASS("r.in.gdal",input="C:/Users/Sam/Documents/R Projects/BuildLeakyDB/flowAccum_xxl.tif",output="flowAccum_xxl")
  execGRASS("r.in.gdal",input="C:/Users/Sam/Documents/R Projects/BuildLeakyDB/streamsRast_xxl.tif",output="streamsRast_xxl")
  
  writeVECT(SDF=allCoords_sdf,vname="allCoords",v.in.ogr_flags = c("o", "overwrite"))

  #this fucker drops the tables  
  execGRASS("r.stream.snap",input="allCoords",output="allCoords_snap",stream_rast="streamsRast_xxl",accumulation="flowAccum_xxl",radius=allowed_radius,threshold=flowAcc_threshold,flags="overwrite")

  allCoords_joinKey=readVECT("allCoords")@data
  
  allCoords_snap=grassTableToDF(execGRASS("v.report",map="allCoords_snap",option="coor",intern=T))
  
  allCoords_snap$cat=as.numeric(allCoords_snap$cat)
  allCoords_write=left_join(allCoords_snap,allCoords_joinKey)
  
  for(i in 1:nrow(allCoords_write)){
  result=dbSendStatement(db_source,paste0("UPDATE Coordinates SET X = '",allCoords_write$x[i],"', Y = '",allCoords_write$y[i],"' WHERE CoordinateIDX = '",allCoords_write$CoordinateIDX[i],"'" ))
  t=dbClearResult(result)
  }
}