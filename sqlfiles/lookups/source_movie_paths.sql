select
  distinct(ao.assetID)
  , ai.path
  , ao.packageName
from
  CTI_AppSrvCatalog.dbo.app_Offerings ao
  INNER JOIN CTI_AppSrvCatalog.dbo.app_Items ai on ai.offeringId = ao.offeringId