countChannelsByTransect=function(){
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
  
  rawWidth=read_csv("C:/Users/Sam/Documents/LeakyRivers/Data/width/CoWidths_fine.csv")[-1,]
  
  #add dep area as pct, and remove extra stuff
  keepNames=c("Date","Site","Transect","Wetted width","Bank-full width","Depositional area units","Channel type","Device","Who","Comments","DepArea_pct","Meters from ZERO pt")
  widths=calcDepPct(rawWidth)[keepNames]
  widths$ReachName=widths$Site
  
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
  
  widths$variable=as.character(widths$variable)
  widths$variable[widths$variable=="Wetted width"]="wettedWidth"
  widths$variable[widths$variable=="Bank-full width"]="bankfullWidth"
  widths$variable[widths$variable=="DepArea_pct"]="depositionalArea"
  
  widths$Site=as.character(widths$Site)
  ww=widths[widths$variable=="wettedWidth",c("Site","Transect","value")]
  
  ww_cc=aggregate(ww$value,by=list(Site=ww$Site,Transect=ww$Transect),FUN=length)
  return(ww_cc)
}

