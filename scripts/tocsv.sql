
create or replace view hcaf_sd_view as SELECT s.CsquareCode,s.OceanArea,s.CenterLat,s.CenterLong,d.FAOAreaM,DepthMin,DepthMax,SSTAnMean,SBTAnMean,SalinityMean, SalinityBMean,PrimProdMean,IceConAnn,d.LandDist,s.EEZFirst,s.LME,d.DepthMean FROM HCAF_S as s INNER JOIN HCAF_D as d ON s.CSquareCode=d.CSquareCode where d.oceanarea > 0;

copy (select * from hcaf_sd_view order by csquarecode) to '/tmp/hcaf.csv' csv;

copy (SELECT speciesid || ':' || lifestage, Layer,SpeciesID,FAOAreas,Pelagic,NMostLat,SMostLat,WMostLong,EMostLong,DepthMin,DepthMax,DepthPrefMin,DepthPrefMax,TempMin,TempMax,TempPrefMin,TempPrefMax,SalinityMin,SalinityMax,SalinityPrefMin,SalinityPrefMax,PrimProdMin,PrimProdMax,PrimProdPrefMin,PrimProdPrefMax,IceConMin,IceConMax,IceConPrefMin,IceConPrefMax,LandDistMin,LandDistMax,LandDistPrefMin,MeanDepth,LandDistPrefMax,LandDistYN FROM hspen) to '/tmp/hspen.csv' csv;
