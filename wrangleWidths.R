#wrangele the width data that mike and I collected
library(tidyverse)
library(reshape2)
rawWidth=read_csv("C:/Users/Sam/Documents/LeakyRivers/Data/width/CoWidths.csv")[-1,]
rawSites=read_csv("C:/Users/Sam/Documents/LeakyRivers/Data/width/widthSites.csv")

#check that all width sites are in sites list:
all((unique(rawWidth$Site) %in% unique(rawSites$Sites)))

#add dep area as pct, and remove extra data
keepNames=c("Date","Site","Transect","Wetted width","Bank-full width","Depositional area units","Channel type","Device","Who","Comments","DepArea_pct")

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

widths=calcDepPct(rawWidth)[keepNames]
widths$ReachName=widths$Site
widths$PointName=paste(widths$ReachName,widths$Transect,sep=' #')
widths=left_join(widths,rawSites[c("Sites","X","Y","Network")],by=c("ReachName" = "Sites"))
w_long=as.tibble(melt(widths,measure.vars=c("Wetted width","Bank-full width","DepArea_pct")))
w_long$Value=as.numeric(w_long$value)
w_long=w_long[!is.na(w_long$Value),]
w_long$dataType=""
w_long$unit=""
w_long$dataType[w_long$variable=="DepArea_pct"]=w_long$`Depositional area units`[w_long$variable=="DepArea_pct"]
w_long$unit[w_long$variable=="DepArea_pct"]='percent cover'
w_long$dataType[w_long$variable!="DepArea_pct"]=w_long$Device[w_long$variable!="DepArea_pct"]
w_long$unit[w_long$variable!="DepArea_pct"]='meters'
w_long$dataType[w_long$dataType=='m2']='Estimated as total area'
w_long$dataType[w_long$dataType=='%']='Estimated as pct cover'
head(w_long)
write.csv(w_long,"C:/Users/Sam/Documents/LeakyRivers/Data/width/FinalWidths_11_2018.csv")
