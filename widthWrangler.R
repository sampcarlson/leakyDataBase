wrangleWidths=function(){
  library(tidyverse)
  library(reshape2)
  
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
  
  #prepare Width data...
  
  rawWidth=read_csv("C:/Users/Sam/Documents/LeakyRivers/Data/width/CoWidths.csv")[-1,]
  rawSites=read_csv("C:/Users/Sam/Documents/LeakyRivers/Data/width/widthSites.csv")
  
  #check that all width sites are in sites list:
  all((unique(rawWidth$Site) %in% unique(rawSites$Sites)))
  
  #add dep area as pct, and remove extra stuff
  keepNames=c("Date","Site","Transect","Wetted width","Bank-full width","Depositional area units","Channel type","Device","Who","Comments","DepArea_pct","Meters from ZERO pt")
  widths=calcDepPct(rawWidth)[keepNames]
  widths$ReachName=widths$Site
  widths$PointName=paste(widths$ReachName,widths$Transect,sep=' #')
  widths=as.data.frame(left_join(widths,rawSites[c("Sites","X","Y","Network")],by=c("ReachName" = "Sites")))
  
  widths$`Meters from ZERO pt`=NULL
  w_long=as.tibble(melt(widths,measure.vars=c("Wetted width","Bank-full width","DepArea_pct")))
  w_long$value=as.numeric(w_long$value)
  w_long=w_long[!is.na(w_long$value),]
  w_long$dataType=""
  w_long$unit=""
  w_long$dataType[w_long$variable=="DepArea_pct"]=w_long$`Depositional area units`[w_long$variable=="DepArea_pct"]
  w_long$unit[w_long$variable=="DepArea_pct"]='percent cover'
  w_long$dataType[w_long$variable!="DepArea_pct"]=w_long$Device[w_long$variable!="DepArea_pct"]
  w_long$unit[w_long$variable!="DepArea_pct"]='meters'
  w_long$dataType[w_long$dataType=='m2']='Estimated as total area'
  w_long$dataType[w_long$dataType=='%']='Estimated as pct cover'
  
  widths=as.data.frame(w_long)
  
  widths=widths[complete.cases(widths[,c("X","Y")]),]
  widths$variable=as.character(widths$variable)
  widths$variable[widths$variable=="Wetted width"]="wettedWidth"
  widths$variable[widths$variable=="Bank-full width"]="bankfullWidth"
  widths$variable[widths$variable=="DepArea_pct"]="depositionalArea"
  
  widths$Site=as.character(widths$Site)
  
  aggDefs=stats::aggregate(widths[,c("Site","variable")],by=list(s=widths$Site,v=widths$variable), FUN=first)[,c("Site","variable")]
  
  aggWidths=data.frame(Site=aggDefs$Site,variable=aggDefs$variable,x=0,y=0,value=0,dataType="",unit="",date="",stringsAsFactors = F)
  for(r in 1:nrow(aggWidths)){
    thisData=widths[(widths$Site==aggWidths$Site[r] & widths$variable==aggWidths$variable[r]),]
    thisData$variable=as.character(thisData$variable)
    thisData$value=as.numeric(thisData$value)
    thisData$dataType=as.character(thisData$dataType)
    thisData$unit=as.character(thisData$unit)
    
    aggWidths$x[r]=thisData$X[1]
    aggWidths$y[r]=thisData$Y[1]
    
    if(aggWidths$variable[r] %in% c("wettedWidth","bankfullWidth")){
      aggWidths$value[r]=mean(stats::aggregate(thisData$value,by=list(t=thisData$Transect),FUN=sum)$x)
    }
    if(aggWidths$variable[r] %in% c("depositionalArea")){
      aggWidths$value[r]=mean(stats::aggregate(thisData$value,by=list(t=thisData$Transect),FUN=mean)$x)
    }
    
    aggWidths$dataType[r]=thisData$dataType[1]
    aggWidths$unit[r]=thisData$unit[1]
    aggWidths$date=as.Date(as.character( thisData$Date[1] ), format="%m/%d/%Y")
    
  }
  
  
  return(aggWidths)
}


widthAreaLengths=function(){
  library(tidyverse)
  library(reshape2)
  
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
  
  #prepare Width data...
  
  rawWidth=read_csv("C:/Users/Sam/Documents/LeakyRivers/Data/width/CoWidths.csv")[-1,]
  rawSites=read_csv("C:/Users/Sam/Documents/LeakyRivers/Data/width/widthSites.csv")
  
  #check that all width sites are in sites list:
  all((unique(rawWidth$Site) %in% unique(rawSites$Sites)))
  
  #add dep area as pct, and remove extra stuff
  keepNames=c("Date","Site","Transect","Wetted width","Bank-full width","Depositional area units","Channel type","Device","Who","Comments","DepArea_pct","Meters from ZERO pt")
  widths=calcDepPct(rawWidth)[keepNames]
  widths$ReachName=widths$Site
  widths$PointName=paste(widths$ReachName,widths$Transect,sep=' #')
  widths=left_join(widths,rawSites[c("Sites","X","Y","Network")],by=c("ReachName" = "Sites"))
  widths$Site=as.character(widths$Site)
  
  siteLengthDF=data.frame(Site=unique(widths$Site),X=0,Y=0,length=0)
  for(r in 1:nrow(siteLengthDF)){
    thisSiteData=widths[widths$Site==siteLengthDF$Site[r],c("Transect","Meters from ZERO pt","X","Y")]
    thisLength=sum(stats::aggregate(thisSiteData,by=list(t=thisSiteData$Transect),FUN=mean)$`Meters from ZERO pt`)
    siteLengthDF$length[r]=thisLength
    siteLengthDF$X[r]=thisSiteData$X[1]
    siteLengthDF$Y[r]=thisSiteData$Y[1]
  }
  return(siteLengthDF)
}

#write.csv(widthAreaLengths(),"widthSiteLengths.csv")
