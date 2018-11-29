library(rgrass7)
library(sp)

delete_data=function(db,table){
  dbExecute(db,paste0("DELETE FROM ",table))
}

addWatershedDefinitions=function(wshedDefs){
  InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif",grassRasterName="streamRast")
  writeVECT(SDF=SpatialPointsDataFrame(coords=wshedDefs[,c("X","Y")],data=wshedDefs[,c("WshedName","id")]),"wshedOutPoints",v.in.ogr_flags=c("o","quiet"))
  execGRASS("r.stream.snap",input="wshedOutPoints",stream_rast="streamRast",radius=10,output="wshedOutPoints_snap",flags="quiet")
  execGRASS("v.db.addtable",map="wshedOutPoints_snap",flags="quiet")
  execGRASS("v.db.join",map="wshedOutPoints_snap",column="cat",other_table="wshedOutPoints",other_column="cat",flags="quiet")
  wshedDefs=grassTableToDF( execGRASS("v.report",map="wshedOutPoints_snap",option="coor",flags="quiet", intern=T) )
  execGRASS("r.in.gdal",input="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/flowDir_xxl.tif",output="flowDir",flags="quiet")
  
  for(i in 1:nrow(wshedDefs)){
  #calculate and write area:
  execGRASS("r.water.outlet", input="flowDir",output="above_point_temp",coordinates=c(wshedDefs$x[i],wshedDefs$y[i]), flags="overwrite")
  execGRASS("r.to.vect",input="above_point_temp",output="dbWriteMe",type="area",flags="overwrite")
  thisAreaIDX=addArea(EPSG=32613)
  
  #calculate and write point:
  thisPointIDX=addPoint(X=wshedDefs$x[i],Y=wshedDefs$y[i])
  
  #write to watersheds table:
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

addData=function(d){}
addDataType=function(){
  return(dataTypeIDX)
}
addBatch=function(batchIDX){}

addLocation=function(locationIDX){}

addPoint=function(X,Y,EPSG=32613,onStream=T){
  newPointIDX=getNewIDX("Points","pointIDX")
  writeDF=data.frame(pointIDX=newPointIDX,X=X,Y=Y,EPSG=EPSG,onStream=onStream)
  dbWriteTable(leakyDB,"Points",writeDF,append=T)
  return(newPointIDX)
}

convertEPSG=function(locationIDX){}

snapToStream=function(pointIDX){}

addArea=function(EPSG=32613,filePath=shapePath,basePathName="leakyArea_",grassFileName="dbWriteMe"){
  newAreaIDX=getNewIDX("Areas","areaIDX")
  
  shpName=paste0(basePathName,newAreaIDX,".shp")
  fullPathName=paste0(filePath,shpName)
  writeDF=data.frame(areaIDX=newAreaIDX,fileName=fullPathName,EPSG=EPSG)
  dbWriteTable(leakyDB,"Areas",writeDF,append=T)
  
  

  execGRASS("v.out.ogr",input=grassFileName,output=fullPathName,format="ESRI_Shapefile",flags="overwrite")
  return(newAreaIDX)
}

inWatershed=function(locationIDX){}