select
  distinct(ao.assetID) as codigo
  , ao.packageName
  , ao.title as titulo
  , ao.genre as genero
  , ao.movieDuration as duracao
  , substring(ao.productionInfo,1,4) as ano
  , ao.isHD as ishd
from
  CTI_AppSrvCatalog.dbo.app_Offerings ao
  INNER JOIN CTI_AppSrvCatalog.dbo.app_Items ai on ai.offeringId = ao.offeringId