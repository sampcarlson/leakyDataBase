library(rgdal)
library(rgrass7)

source('~/R Projects/mapC/grassTools.R')


#InitGrass_byRaster(rasterPath="C:/Users/Sam/Desktop/spatial/QgisEnvironment/Inputs_and_scripts/allDemRaw/all4_wgs84_13n.tif")
InitGrass_byRaster(rasterPath="C:/Users/Sam/Desktop/spatial/QgisEnvironment/Inputs_and_scripts/allDemRaw/LeakyRiversDEM_rectTrim.tif")

#threshold is in cells, not m^2!!!!
execGRASS("r.watershed",elevation="dem@PERMANENT",threshold=10000,drainage="flowDir_xxl",stream="streams_rast",spi="streamPower",accumulation="flowAccum_xxl", flags=c("overwrite", "a"))
execGRASS("r.thin",input="streams_rast",output="streams_rast",flags="overwrite")
execGRASS("r.to.vect",input="streams_rast",output="streams_vect",type="line")
execGRASS("v.split",input="streams_vect",output="streamSegs_250m_vect",length=250)
execGRASS("v.in.ogr",input="C:/Users/Sam/Documents/R Projects/mapC/AnalysisExtent.shp",output="extent")
execGRASS("v.select",ainput="streamSegs_250m_vect",binput="extent",output="streamSegs_250m_vect_clip",operator="overlap")
execGRASS("v.out.ogr",input="streamSegs_250m_vect_clip",
          output="C:/Users/Sam/Documents/R Projects/BuildLeakyDB/xxl_streamSegs_250m.shp",
          format="ESRI_Shapefile",flags="overwrite")

execGRASS("r.out.gdal",input="flowDir_xxl",output="C:/Users/Sam/Documents/R Projects/BuildLeakyDB/flowDir_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
execGRASS("r.out.gdal",input="flowAccum_xxl",output="C:/Users/Sam/Documents/R Projects/BuildLeakyDB/flowAccum_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
execGRASS("r.out.gdal",input="streams_rast",output="C:/Users/Sam/Documents/R Projects/BuildLeakyDB/streamsRast_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
execGRASS("r.out.gdal",input="streamPower",output="C:/Users/Sam/Documents/R Projects/BuildLeakyDB/streamPower_xxl.tif",nodata=0,format="GTiff",flags="overwrite")
