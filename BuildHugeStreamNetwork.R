library(rgdal)
library(rgrass7)

source("C:/Users/sam/Documents/R/projects/rGrassTools/grassTools.r")
gc()
#InitGrass_byRaster(rasterPath="C:/Users/Sam/Desktop/spatial/QgisEnvironment/Inputs_and_scripts/allDemRaw/all4_wgs84_13n.tif")
InitGrass_byRaster(rasterPath="C:/Users/Sam/Documents/spatial/data/dem/leakyRivers/trim/LeakyRiversDEM_rectTrim_knobFix.tif")

#threshold is in cells, not m^2!!!!
execGRASS("r.watershed",elevation="dem@PERMANENT",threshold=5000,convergence=9,drainage="flowDir_xxl",stream="streams_rast",spi="streamPower",accumulation="flowAccum_xxl", flags=c("overwrite", "a"))
execGRASS("r.thin",input="streams_rast",output="streams_rast",flags="overwrite")
execGRASS("r.to.vect",input="streams_rast",output="streams_vect",type="line",flags="overwrite")
execGRASS("v.split",input="streams_vect",output="streamSegs_250m_vect",length=250,flags="overwrite")
execGRASS("v.in.ogr",input="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/AnalysisExtent.shp",output="extent",flags="overwrite")
execGRASS("v.select",ainput="streamSegs_250m_vect",binput="extent",output="streamSegs_250m_vect_clip",operator="overlap",flags="overwrite")
execGRASS("v.out.ogr",input="streamSegs_250m_vect_clip",
          output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/xxl_streamSegs_250m.shp",
          format="ESRI_Shapefile",flags="overwrite")

execGRASS("r.out.gdal",input="flowDir_xxl",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/flowDir_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
execGRASS("r.out.gdal",input="flowAccum_xxl",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/flowAccum_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
execGRASS("r.out.gdal",input="streams_rast",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamsRast_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
execGRASS("r.out.gdal",input="streamPower",output="C:/Users/Sam/Documents/spatial/r_workspaces/LeakyDB/streamPower_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
