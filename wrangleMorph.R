library(dplyr)
morph_1=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/morph/Geomorph_sites_and_data_bridget.csv",stringsAsFactors = F)

#add pool sediment, join by 'reach'
morph_2=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/morph/Livers_2017_trim.csv",stringsAsFactors = F)[,c("Reach","Pool.sediment.OC..100m.valley..Mg.","Logjam.sediment.OC..100m.valley..Mg.")]
morph_2$totalSedC_Mg_100m_valley=morph_2$Pool.sediment.OC..100m.valley..Mg.+morph_2$Logjam.sediment.OC..100m.valley..Mg.
morph_2$Pool.sediment.OC..100m.valley..Mg.=NULL
morph_2$Logjam.sediment.OC..100m.valley..Mg.=NULL
morph=left_join(morph_1,morph_2,by=c("Reach"="Reach"))


#add valley/channel length, wood surface area, join by length * jams
morph_3=read.csv("C:/Users/Sam/Documents/LeakyRivers/Data/morph/moreMorph.csv",stringsAsFactors = F)[,c("Length..m.","Valley.length..m.","Wood.Surface.Area..m2.","Jams..number.")]
morph=left_join(morph,morph_3,by=c("Length..m."="Length..m.","Jams"="Jams..number."))
write.csv(morph,"morphForDB.csv")
