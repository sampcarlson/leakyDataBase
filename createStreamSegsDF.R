createStreamSegsDF=function(streamSegsPath=shapefile("C:/Users/Sam/Documents/spatial/r_workspaces/leakyDB/xxl_streamSegs.shp",)){
  c=2 #number of cores to use in parallel
  library(rgeos)
  library(raster)
  library(parallel)
  library(snow)
  library(dplyr)
  
  
  print("load stream segs shape...")
  streamSegs=shapefile(streamSegsPath)
  #as compared to Q, the data is stored in attributes, and the attributes are stored as data
  #field 'AUTO' is unique segment ID
  
  getCoordCount=function(subFeature){
    return(nrow(subFeature@Lines[[1]]@coords))
  }
  
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
  cats=execGRASS("v.category",input="streamSegs",option="print",intern = T)
  writeDF=data.frame(p="P",pid=cats,cats=cats,offset="50%")
  write.table(writeDF,file="vSeg.txt",row.names=F,col.names = F,quote=F)
  execGRASS("v.segment",input="streamSegs",output="segPoints",rules=paste(getwd(),"vSeg.txt",sep="/"),flags=c("overwrite","verbose"))
  
  execGRASS("v.out.ogr",input="segPoints",
            output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/segPoints.shp",
            format="ESRI_Shapefile",flags="overwrite")
  
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
}