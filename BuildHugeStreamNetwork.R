library(rgdal)
library(rgrass7)
buildHugeStreamNetwork=function(segLength){
  source("C:/Users/sam/Documents/R/projects/rGrassTools/grassTools.r")
  gc()
  #InitGrass_byRaster(rasterPath="C:/Users/Sam/Desktop/spatial/QgisEnvironment/Inputs_and_scripts/allDemRaw/all4_wgs84_13n.tif")
  InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/data/dem/leakyRivers/trim/LeakyRiversDEM_rectTrim_knobFix.tif")
  
  #threshold is in cells, not m^2!!!!
  execGRASS("r.watershed",elevation="dem@PERMANENT",threshold=5000,drainage="flowDir_xxl",stream="streams_rast",spi="streamPower",accumulation="flowAccum_xxl", flags=c("overwrite", "a", "s"))
  execGRASS("r.thin",input="streams_rast",output="streams_rast",flags="overwrite")
  execGRASS("r.to.vect",input="streams_rast",output="streams_vect",type="line",flags="overwrite")
  execGRASS("v.split",input="streams_vect",output="streamSegs_vect",length=segLength,flags="overwrite")
  
  execGRASS("v.in.ogr",input="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/AnalysisExtent.shp",output="extent",flags="overwrite")
  execGRASS("v.select",ainput="streamSegs_vect",binput="extent",output="streamSegs_vect_clip",operator="overlap",flags="overwrite")
  execGRASS("v.category",input="streamSegs_vect_clip",option="del",cat=-1,output="streamSegsNoCat")
  execGRASS("v.category",input="streamSegsNoCat",output="streamSegsCat",option="add",flags="overwrite")
  execGRASS("v.out.ogr",input="streamSegsCat",
            output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/xxl_streamSegs.shp",
            format="ESRI_Shapefile",flags="overwrite")
  
  
  cats=execGRASS("v.category",input="streamSegsCat",option="print",intern = T)
  writeDF=data.frame(p="P",pid=cats,cats=cats,offset="50%")
  write.table(writeDF,file="vSeg.txt",row.names=F,col.names = F,quote=F)
  execGRASS("v.segment",input="streamSegsCat",output="segPoints",rules=paste(getwd(),"vSeg.txt",sep="/"),flags=c("overwrite","verbose"))
  
  execGRASS("v.out.ogr",input="segPoints",
            output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/segPoints.shp",
            format="ESRI_Shapefile",flags="overwrite")
  
  execGRASS("r.slope.aspect",elevation="dem@PERMANENT",slope="slope",aspect="aspect")
  
  execGRASS("r.out.gdal",input="slope",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/slope_xxl.tif",nodata=-999,format="GTiff",flags="overwrite")
  execGRASS("r.out.gdal",input="aspect",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/aspect_xxl.tif",nodata=-999,format="GTiff",flags="overwrite")
  execGRASS("r.out.gdal",input="flowDir_xxl",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/flowDir_xxl.tif",nodata=-999,format="GTiff",flags="overwrite")
  execGRASS("r.out.gdal",input="flowAccum_xxl",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/flowAccum_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
  execGRASS("r.out.gdal",input="streams_rast",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
  execGRASS("r.out.gdal",input="streamPower",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamPower_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
}


