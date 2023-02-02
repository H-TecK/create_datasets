#!/usr/bin/env bash

# Set environment vars here
echo "*** Setting Environment Variables ***"
export PROJECT_ID=$(gcloud config get-value project)
export BUCKET_NAME="sap-cortex-labs"

# bq mk --dataset CDC_PROCESSED
# bq mk --dataset SAP_REPLICATED_DATA
# bq mk --dataset SAP_REPORTING

# Create Tables for Dataset - SAP_REPLICATED_DATA
# Using JSON file to get the schema for Tables
gsutil cp -r gs://$BUCKET_NAME/* .

bq mk --table SAP_REPLICATED_DATA.mara schema_sap_replicated_data/mara.json
bq mk --table SAP_REPLICATED_DATA.makt schema_sap_replicated_data/makt.json
bq mk --table SAP_REPLICATED_DATA.vbak schema_sap_replicated_data/vbak.json
bq mk --table SAP_REPLICATED_DATA.vbap schema_sap_replicated_data/vbap.json
bq mk --table SAP_REPLICATED_DATA.vbfa schema_sap_replicated_data/vbfa.json
bq mk --table SAP_REPLICATED_DATA.vbup schema_sap_replicated_data/vbup.json

# Create Tables and loading data for Dataset - CDC_PROCESSED
# Using JSON file to get the schema for Tables
bq load --source_format=CSV CDC_PROCESSED.makt gs://$BUCKET_NAME/upload_files_cdc_processed/makt.csv schema_cdc_processed/makt.json
bq load --source_format=CSV CDC_PROCESSED.mara gs://$BUCKET_NAME/upload_files_cdc_processed/mara.csv schema_cdc_processed/mara.json
bq load --source_format=CSV CDC_PROCESSED.vbak gs://$BUCKET_NAME/upload_files_cdc_processed/vbak.csv schema_cdc_processed/vbak.json
bq load --source_format=CSV CDC_PROCESSED.vbap gs://$BUCKET_NAME/upload_files_cdc_processed/vbap.csv schema_cdc_processed/vbap.json
bq load --source_format=CSV CDC_PROCESSED.vbfa gs://$BUCKET_NAME/upload_files_cdc_processed/vbfa.csv schema_cdc_processed/vbfa.json
bq load --source_format=CSV CDC_PROCESSED.vbup gs://$BUCKET_NAME/upload_files_cdc_processed/vbup.csv schema_cdc_processed/vbup.json

bq query \
--use_legacy_sql=false \
"CREATE OR REPLACE VIEW CDC_PROCESSED.vbap_view
AS(
  WITH S1 AS (
      SELECT * FROM SAP_REPLICATED_DATA.vbap order by recordstamp
  ),
    T1 AS (
    SELECT MANDT,VBELN,POSNR, max(recordstamp) as recordstamp from SAP_REPLICATED_DATA.vbap where operation_flag in ('U', 'I') group by MANDT,VBELN,POSNR order by recordstamp
    ),
    D1 AS (
        SELECT DT1.MANDT,DT1.VBELN,DT1.POSNR, DT1.recordstamp from SAP_REPLICATED_DATA.vbap DT1,T1 where DT1.operation_flag ='D' and DT1.MANDT=T1.MANDT and DT1.VBELN=T1.VBELN and DT1.POSNR=T1.POSNR and DT1.recordstamp > T1.recordstamp order by recordstamp
    ),
    T1S1 AS (
    SELECT  S1.* EXCEPT(operation_flag,is_deleted) from S1 INNER JOIN T1 ON T1.MANDT=S1.MANDT and T1.VBELN=S1.VBELN and T1.POSNR=S1.POSNR and S1.recordstamp=T1.recordstamp
    )
    SELECT T1S1.* EXCEPT(recordstamp) from T1S1 where MANDT not in (SELECT MANDT from D1) and VBELN not in (SELECT VBELN from D1) and POSNR not in (SELECT POSNR from D1)
  )"





# Creates the VIEWS in the Datasets - SAP_REPORTING

bq mk \
--use_legacy_sql=false \
--view \
'SELECT   
MARA.MANDT AS Client_MANDT,  MARA.MATNR AS MaterialNumber_MATNR,  MARA.ERSDA AS CreatedOn_ERSDA,  MARA.ERNAM AS NameOfPersonWhoCreatedTheObject_ERNAM,  MARA.LAEDA AS DateOfLastChange_LAEDA,
MARA.AENAM AS NameOfPersonWhoChangedObject_AENAM,  MARA.VPSTA AS MaintenanceStatusOfCompleteMaterial_VPSTA,  MARA.PSTAT AS MaintenanceStatus_PSTAT,  MARA.LVORM AS FlagMaterialForDeletionAtClientLevel_LVORM,
MARA.MTART AS MaterialType_MTART,  MARA.MBRSH AS IndustrySector_MBRSH,  MARA.MATKL AS MaterialGroup_MATKL,  MARA.BISMT AS OldMaterialNumber_BISMT,  MARA.MEINS AS BaseUnitOfMeasure_MEINS,  MARA.BSTME AS OrderUnit_BSTME,
MARA.ZEINR AS DocumentNumber__withoutDocumentManagementSystem___ZEINR,  MARA.ZEIAR AS DocumentType__withoutDocumentManagementSystem___ZEIAR,  MARA.ZEIVR AS DocumentVersion__withoutDocumentManagementSystem___ZEIVR,
MARA.ZEIFO AS PageFormatOfDocument__withoutDocumentManagementSystem___ZEIFO,  MARA.AESZN AS DocumentChangeNumber__withoutDocumentManagementSystem___AESZN,  MARA.BLATT AS PageNumberOfDocument__withoutDocumentManagementSystem___BLATT,
MARA.BLANZ AS NumberOfSheets__withoutDocumentManagementSystem___BLANZ,  MARA.FERTH AS ProductioninspectionMemo_FERTH,  MARA.FORMT AS PageFormatOfProductionMemo_FORMT,  MARA.GROES AS Sizedimensions_GROES,
MARA.WRKST AS BasicMaterial_WRKST,  MARA.NORMT AS IndustryStandardDescription__suchAsAnsiOrIso___NORMT,  MARA.LABOR AS LaboratorydesignOffice_LABOR,  MARA.EKWSL AS PurchasingValueKey_EKWSL,  MARA.BRGEW AS GrossWeight_BRGEW,
MARA.NTGEW AS NetWeight_NTGEW,  MARA.GEWEI AS WeightUnit_GEWEI,  MARA.VOLUM AS Volume_VOLUM,  MARA.VOLEH AS VolumeUnit_VOLEH,  MARA.BEHVO AS ContainerRequirements_BEHVO,  MARA.RAUBE AS StorageConditions_RAUBE,
MARA.TEMPB AS TemperatureConditionsIndicator_TEMPB,  MARA.DISST AS LowLevelCode_DISST,  MARA.TRAGR AS TransportationGroup_TRAGR,  MARA.STOFF AS HazardousMaterialNumber_STOFF,  MARA.SPART AS Division_SPART,
MARA.KUNNR AS Competitor_KUNNR,  MARA.EANNR AS EuropeanArticleNumber_EANNR,  MARA.WESCH AS Quantity_NumberOfGrgiSlipsToBePrinted_WESCH,  MARA.BWVOR AS ProcurementRule_BWVOR,  MARA.BWSCL AS SourceOfSupply_BWSCL,
MARA.SAISO AS SeasonCategory_SAISO,  MARA.ETIAR AS LabelType_ETIAR,  MARA.ETIFO AS LabelForm_ETIFO,  MARA.ENTAR AS Deactivated_ENTAR,  MARA.EAN11 AS InternationalArticleNumber__eanupc___EAN11,
MARA.NUMTP AS CategoryOfInternationalArticleNumber__ean___NUMTP,  MARA.LAENG AS Length_LAENG,  MARA.BREIT AS Width_BREIT,  MARA.HOEHE AS Height_HOEHE,  MARA.MEABM AS UnitOfDimensionForLengthwidthheight_MEABM,
MARA.PRDHA AS ProductHierarchy_PRDHA,  MARA.AEKLK AS StockTransferNetChangeCosting_AEKLK,  MARA.CADKZ AS CadIndicator_CADKZ,  MARA.QMPUR AS QmInProcurementIsActive_QMPUR,  MARA.ERGEW AS AllowedPackagingWeight_ERGEW,
MARA.ERGEI AS UnitOfWeight__allowedPackagingWeight___ERGEI,  MARA.ERVOL AS AllowedPackagingVolume_ERVOL,  MARA.ERVOE AS VolumeUnit__allowedPackagingVolume___ERVOE,  MARA.GEWTO AS ExcessWeightToleranceForHandlingUnit_GEWTO,
MARA.VOLTO AS ExcessVolumeToleranceOfTheHandlingUnit_VOLTO,  MARA.VABME AS VariablePurchaseOrderUnitActive_VABME,  MARA.KZREV AS RevisionLevelHasBeenAssignedToTheMaterial_KZREV,  MARA.KZKFG AS ConfigurableMaterial_KZKFG,
MARA.XCHPF AS BatchManagementRequirementIndicator_XCHPF,  MARA.VHART AS PackagingMaterialType_VHART,  MARA.FUELG AS MaximumLevel__byVolume___FUELG,  MARA.STFAK AS StackingFactor_STFAK,
MARA.MAGRV AS MaterialGroup_PackagingMaterials_MAGRV,  MARA.BEGRU AS AuthorizationGroup_BEGRU,  MARA.DATAB AS ValidFromDate_DATAB,  MARA.LIQDT AS DeletionDate_LIQDT,  MARA.SAISJ AS SeasonYear_SAISJ,
MARA.PLGTP AS PriceBandCategory_PLGTP,  MARA.MLGUT AS EmptiesBillOfMaterial_MLGUT,  MARA.EXTWG AS ExternalMaterialGroup_EXTWG,  MARA.SATNR AS CrossPlantConfigurableMaterial_SATNR,  MARA.ATTYP AS MaterialCategory_ATTYP,
MARA.KZKUP AS Indicator_MaterialCanBeCoProduct_KZKUP,  MARA.KZNFM AS Indicator_TheMaterialHasAFollowUpMaterial_KZNFM,  MARA.PMATA AS PricingReferenceMaterial_PMATA,  MARA.MSTAE AS CrossPlantMaterialStatus_MSTAE,
MARA.MSTAV AS CrossDistributionChainMaterialStatus_MSTAV,  MARA.MSTDE AS DateFromWhichTheCrossPlantMaterialStatusIsValid_MSTDE,  MARA.MSTDV AS DateFromWhichTheXDistrChainMaterialStatusIsValid_MSTDV,
MARA.TAKLV AS TaxClassificationOfTheMaterial_TAKLV,  MARA.RBNRM AS CatalogProfile_RBNRM,  MARA.MHDRZ AS MinimumRemainingShelfLife_MHDRZ,  MARA.MHDHB AS TotalShelfLife_MHDHB,  MARA.MHDLP AS StoragePercentage_MHDLP,
MARA.INHME AS ContentUnit_INHME,  MARA.INHAL AS NetContents_INHAL,  MARA.VPREH AS ComparisonPriceUnit_VPREH, MARA.INHBR AS GrossContents_INHBR,
MARA.CMETH AS QuantityConversionMethod_CMETH,  MARA.CUOBF AS InternalObjectNumber_CUOBF,  MARA.KZUMW AS EnvironmentallyRelevant_KZUMW,  MARA.KOSCH AS ProductAllocationDeterminationProcedure_KOSCH,
MARA.SPROF AS PricingProfileForVariants_SPROF,  MARA.NRFHG AS MaterialQualifiesForDiscountInKind_NRFHG,  MARA.MFRPN AS ManufacturerPartNumber_MFRPN,  MARA.MFRNR AS ManufacturerNumber_MFRNR,
MARA.BMATN AS NumberInventoryManagedMaterial_BMATN,  MARA.MPROF AS MfrPartProfile_MPROF,  MARA.KZWSM AS UnitsOfMeasureUsage_KZWSM,  MARA.SAITY AS RolloutInASeason_SAITY,
MARA.PROFL AS DangerousGoodsIndicatorProfile_PROFL,  MARA.IHIVI AS Indicator_HighlyViscous_IHIVI,  MARA.ILOOS AS Indicator_InBulkliquid_ILOOS,  MARA.SERLV AS LevelOfExplicitnessForSerialNumber_SERLV,
MARA.KZGVH AS PackagingMaterialIsClosedPackaging_KZGVH,  MARA.XGCHP AS Indicator_ApprovedBatchRecordRequired_XGCHP,  MARA.KZEFF AS AssignEffectivityParameterValuesOverrideChangeNumbers_KZEFF,
MARA.COMPL AS MaterialCompletionLevel_COMPL,  MARA.IPRKZ AS PeriodIndicatorForShelfLifeExpirationDate_IPRKZ,  MARA.RDMHD AS RoundingRuleForCalculationOfSled_RDMHD,  MARA.PRZUS AS Indicator_ProductCompositionPrintedOnPackaging_PRZUS,
MARA.MTPOS_MARA AS GeneralItemCategoryGroup_MTPOS_MARA,  MARA.BFLME AS GenericMaterialWithLogisticalVariants_BFLME,  MARA.MATFI AS MaterialIsLocked_MATFI,  MARA.CMREL AS RelevantForConfigurationManagement_CMREL,
MARA.BBTYP AS AssortmentListType_BBTYP,  MARA.SLED_BBD AS ExpirationDate_SLED_BBD,  MARA.GTIN_VARIANT AS GlobalTradeItemNumberVariant_GTIN_VARIANT,  MARA.GENNR AS MaterialNumberOfTheGenericMaterialInPrepackMaterials_GENNR,
MARA.RMATP AS ReferenceMaterialForMaterialsPackedInSameWay_RMATP,  MARA.GDS_RELEVANT AS Indicator_GlobalDataSynchronizationRelevant_GDS_RELEVANT,  MARA.WEORA AS AcceptanceAtOrigin_WEORA,
MARA.HUTYP_DFLT AS StandardHuType_HUTYP_DFLT,  MARA.PILFERABLE AS Pilferable_PILFERABLE,  MARA.WHSTC AS WarehouseStorageCondition_WHSTC,  MARA.WHMATGR AS WarehouseMaterialGroup_WHMATGR,  MARA.HNDLCODE AS HandlingIndicator_HNDLCODE,
MARA.HAZMAT AS RelevantForHazardousSubstances_HAZMAT,  MARA.HUTYP AS HandlingUnitType_HUTYP,  MARA.TARE_VAR AS VariableTareWeight_TARE_VAR,  MARA.MAXC AS MaximumAllowedCapacityOfPackagingMaterial_MAXC,
MARA.MAXC_TOL AS OvercapacityToleranceOfTheHandlingUnit_MAXC_TOL,  MARA.MAXL AS MaximumPackingLengthOfPackagingMaterial_MAXL,  MARA.MAXB AS MaximumPackingWidthOfPackagingMaterial_MAXB,
MARA.MAXH AS MaximumPackingHeightOfPackagingMaterial_MAXH,  MARA.MAXDIM_UOM AS UnitOfMeasureForMaximumPackingLengthwidthheight_MAXDIM_UOM,  MARA.HERKL AS CountryOfOriginOfMaterial_HERKL,
MARA.MFRGR AS MaterialFreightGroup_MFRGR,  MARA.QQTIME AS QuarantinePeriod_QQTIME,  MARA.QQTIMEUOM AS TimeUnitForQuarantinePeriod_QQTIMEUOM,  MARA.QGRP AS QualityInspectionGroup_QGRP,  MARA.SERIAL AS SerialNumberProfile_SERIAL,
MARA.PS_SMARTFORM AS FormName_PS_SMARTFORM,  MARA.LOGUNIT AS EwmCw_LogisticsUnitOfMeasure_LOGUNIT,  MARA.CWQREL AS EwmCw_MaterialIsACatchWeightMaterial_CWQREL,  MARA.CWQPROC AS EwmCw_CatchWeightProfileForEnteringCwQuantity_CWQPROC,
MARA.CWQTOLGR AS EwmCatchWeightToleranceGroupForEwm_CWQTOLGR,  MARA.ADPROF AS AdjustmentProfile_ADPROF,  MARA.IPMIPPRODUCT AS IdForAnIntellectualProperty__crmProduct___IPMIPPRODUCT,
MARA.ALLOW_PMAT_IGNO AS VariantPriceAllowed__forMaterialMaster___ALLOW_PMAT_IGNO,  MARA.MEDIUM AS Medium_MEDIUM,  MARA.COMMODITY AS PhysicalCommodity_COMMODITY,
MAKT.SPRAS AS Language_SPRAS,  MAKT.MAKTX AS MaterialText_MAKTX
FROM   CDC_PROCESSED.mara as mara
join   CDC_PROCESSED.makt as makt on mara.mandt = makt.mandt and mara.matnr = makt.matnr
' \
SAP_REPORTING.MaterialsMD


###

bq mk \
--use_legacy_sql=false \
--view \
"select SO.mandt as Client_MANDT, SO.VBELV as SalesOrder_VBELV, SO.POSNV as SalesItem_POSNV,
        Deliveries.VBELV as DeliveryNumber_VBELV, Deliveries.POSNV as DeliveryItem_POSNV,  
       Deliveries.VBELN as InvoiceNumber_VBELN, Deliveries.POSNN as InvoiceItem_POSNN,
       SO.RFMNG as DeliveredQty_RFMNG, SO.MEINS as DeliveredUoM_MEINS, 
       Deliveries.RFMNG as InvoiceQty_RFMNG, Deliveries.MEINS as  InvoiceUoM_MEINS,
       Deliveries.RFWRT as InvoiceValue_RFWRT, Deliveries.WAERS as InvoiceCurrency_WAERS
from CDC_PROCESSED.vbfa as SO 
left outer join CDC_PROCESSED.vbfa as Deliveries 
on  SO.VBELN = Deliveries.VBELV and SO.mandt = Deliveries.mandt and SO.POSNN = Deliveries.POSNV
where SO.vbtyp_V = 'C'
and SO.vbtyp_n in ('J' ,'T')
and Deliveries.vbtyp_n in ('M')
order by SO.VBELV" \
SAP_REPORTING.SDDocumentFlow

###

bq mk \
--use_legacy_sql=false \
--view \
"select VBUP.MANDT AS Client_MANDT,  VBUP.VBELN AS SDDocumentNumber_VBELN, VBUP.POSNR AS ItemNumberOfTheSdDocument_POSNR,
 VBUP.RFSTA AS ReferenceStatus_RFSTA,  VBUP.RFGSA AS OverallStatusOfReference_RFGSA,
VBUP.BESTA AS ConfirmationStatusOfDocumentItem_BESTA,  VBUP.LFSTA AS DeliveryStatus_LFSTA,  VBUP.LFGSA AS OverallDeliveryStatusOfTheItem_LFGSA,  VBUP.WBSTA AS GoodsMovementStatus_WBSTA,
VBUP.FKSTA AS BillingStatusOfDelivery_FKSTA,  VBUP.FKSAA AS BillingStatusForOrder_FKSAA,  VBUP.ABSTA AS RejectionStatusForSdItem_ABSTA,
VBUP.GBSTA AS OverallProcessingStatusOfTheSdDocumentItem_GBSTA,  VBUP.KOSTA AS PickingStatusputawayStatus_KOSTA,  VBUP.LVSTA AS StatusOfWarehouseManagementActivities_LVSTA,  VBUP.UVALL AS GeneralIncompletionStatusOfItem_UVALL,
VBUP.UVVLK AS IncompletionStatusOfTheItemWithRegardToDelivery_UVVLK,  VBUP.UVFAK AS ItemIncompletionStatusWithRespectToBilling_UVFAK,  VBUP.UVPRS AS PricingForItemIsIncomplete_UVPRS,  VBUP.FKIVP AS IntercompanyBillingStatus_FKIVP,
VBUP.UVP01 AS CustomerReserves1_ItemStatus_UVP01,  VBUP.UVP02 AS CustomerReserves2_ItemStatus_UVP02,  VBUP.UVP03 AS ItemReserves3_ItemStatus_UVP03,  VBUP.UVP04 AS ItemReserves4_ItemStatus_UVP04,
VBUP.UVP05 AS CustomerReserves5_ItemStatus_UVP05,  VBUP.PKSTA AS PackingStatusOfItem_PKSTA,  VBUP.KOQUA AS ConfirmationStatusOfPickingputaway_KOQUA, 
VBUP.CMPPI AS StatusOfCreditCheckAgainstFinancialDocument_CMPPI,  VBUP.CMPPJ AS StatusOfCreditCheckAgainstExportCreditInsurance_CMPPJ,  VBUP.UVPIK AS IncompleteStatusOfItemForPickingputaway_UVPIK,
VBUP.UVPAK AS IncompleteStatusOfItemForPackaging_UVPAK,  VBUP.UVWAK AS IncompleteStatusOfItemRegardingGoodsIssue_UVWAK,  VBUP.DCSTA AS DelayStatus_DCSTA,  VBUP.RRSTA AS RevenueDeterminationStatus_RRSTA,
VBUP.VLSTP AS DecentralizedWhseProcessing_VLSTP,  VBUP.FSSTA AS BillingBlockStatusForItems_FSSTA,  VBUP.LSSTA AS DeliveryBlockStatusForItem_LSSTA,  VBUP.PDSTA AS PodStatusOnItemLevel_PDSTA,
VBUP.MANEK AS ManualCompletionOfContract_MANEK,  VBUP.HDALL AS InboundDeliveryItemNotYetComplete__onHold___HDALL,  VBUP.LTSPS AS Indicator_StockableTypeSwitchedIntoStandardProduct_LTSPS,
VBUP.FSH_AR_STAT_ITM AS AllocationStatusOfASalesDocumentItem_FSH_AR_STAT_ITM,  VBUP.MILL_VS_VSSTA AS StatusOfSalesOrderItem_MILL_VS_VSSTA,
( case VBUP.FKSTA 
    when 'A' then 'Not Yet Processed'
    when 'B' then 'Partially Processed'
    when 'C' then 'Completely Processed' 
    end ) as Billing_Status,
( case VBUP.LFSTA
    when 'A' then 'Not Yet Processed'
    when 'B' then 'Partially Processed'
    when 'C' then 'Completely Processed' 
    end ) as Delivery_Status
from CDC_PROCESSED.vbup AS VBUP" \
SAP_REPORTING.SDStatus_Items

###

bq mk \
--use_legacy_sql=false \
--view \
"select  vbak.MANDT as Client_MANDT, vbak.VBELN as SalesDocument_VBELN, vbap.POSNR as Item_POSNR, vbap.MATNR as MaterialNumber_MATNR, vbak.ERDAT as CreationDate_ERDAT, vbak.ERZET as CreationTime_ERZET, 
vbak.ERNAM as CreatedBy_ERNAM, vbak.ANGDT as QuotationDateFrom_ANGDT, vbak.BNDDT as QuotationDateTo_BNDDT, vbak.AUDAT as DocumentDate_AUDAT, 
vbak.VBTYP as DocumentCategory_VBTYP, vbak.TRVOG as TransactionGroup_TRVOG, vbak.AUART as SalesDocumentType_AUART, vbak.AUGRU as Reason_AUGRU, 
vbak.GWLDT as WarrantyDate_GWLDT, vbak.SUBMI as Collectivenumber_SUBMI, vbak.LIFSK as Deliveryblock_LIFSK, vbak.FAKSK as Billingblock_FAKSK, 
vbak.NETWR as NetValueoftheSalesOrderinDocumentCurrency_NETWR, vbak.WAERK as CurrencyHdr_WAERK, vbak.VKORG as SalesOrganization_VKORG, vbak.VTWEG as DistributionChannel_VTWEG, 
vbak.SPART as DivisionHdr_SPART, vbak.VKGRP as SalesGroup_VKGRP, vbak.VKBUR as SalesOffice_VKBUR, vbak.GSBER as BusinessAreaHdr_GSBER, vbak.GSKST as CostCtrBusinessArea_GSKST, 
vbak.GUEBG as AgreementValidFrom_GUEBG, vbak.GUEEN as AgreementValidTo_GUEEN, vbak.KNUMV as ConditionNumber_KNUMV, vbak.VDATU as Requesteddeliverydate_VDATU, 
vbak.VPRGR as Proposeddatetype_VPRGR, vbak.AUTLF as CompleteDeliveryFlag_AUTLF, vbak.VBKLA as OriginalSystem_VBKLA, vbak.VBKLT as DocumentIndicator_VBKLT, 
vbak.KALSM as PricingProcedure_KALSM, vbak.VSBED as ShippingConditions_VSBED, vbak.FKARA as Proposedbillingtype_FKARA, vbak.AWAHR as Salesprobability_AWAHR, 
vbak.KTEXT as Searchtermforproductproposal_KTEXT, vbak.BSTNK as Customerpurchaseordernumber_BSTNK, vbak.BSARK as Customerpurchaseordertype_BSARK, 
vbak.BSTDK as Customerpurchaseorderdate_BSTDK, vbak.BSTZD as Purchaseordernumbersupplement_BSTZD, vbak.IHREZ as YourReference_IHREZ, vbak.BNAME as Nameoforderer_BNAME, 
vbak.TELF1 as TelephoneNumber_TELF1, vbak.MAHZA as Numberofcontactsfromthecustomer_MAHZA, vbak.MAHDT as Lastcustomercontactdate_MAHDT, 
vbak.KUNNR as SoldtoParty_KUNNR, vbak.KOSTL as CostCenterHdr_KOSTL, vbak.STAFO as Updategroupforstatistics_STAFO, vbak.STWAE as Statisticscurrency_STWAE, 
vbak.AEDAT as ChangedOn_AEDAT, vbak.KVGR1 as Customergroup1_KVGR1, vbak.KVGR2 as Customergroup2_KVGR2, vbak.KVGR3 as Customergroup3_KVGR3, 
vbak.KVGR4 as Customergroup4_KVGR4, vbak.KVGR5 as Customergroup5_KVGR5, vbak.KNUMA as Agreement_KNUMA, vbak.KOKRS as ControllingArea_KOKRS, 
vbak.PS_PSP_PNR as WBSElementHdr_PS_PSP_PNR, vbak.KURST as ExchangeRateType_KURST, vbak.KKBER as Creditcontrolarea_KKBER, 
vbak.KNKLI as CustomerCreditLimitRef_KNKLI, vbak.GRUPP as CustomerCreditGroup_GRUPP, vbak.SBGRP as Creditrepresentativegroupforcreditmanagement_SBGRP, 
vbak.CTLPC as Riskcategory_CTLPC, vbak.CMWAE as Currencykeyofcreditcontrolarea_CMWAE, vbak.CMFRE as Releasedateofthedocumentdeterminedbycreditmanagement_CMFRE, 
vbak.CMNUP as Dateofnextcreditcheckofdocument_CMNUP, vbak.CMNGV as Nextdate_CMNGV, vbak.AMTBL as Releasedcreditvalueofthedocument_AMTBL, 
vbak.HITYP_PR as Hierarchytypeforpricing_HITYP_PR, vbak.ABRVW as UsageIndicator_ABRVW, vbak.ABDIS as MRPfordeliveryscheduletypes_ABDIS, 
vbak.VGBEL as Documentnumberofthereferencedocument_VGBEL, vbak.OBJNR as Objectnumberatheaderlevel_OBJNR, vbak.BUKRS_VF as Companycodetobebilled_BUKRS_VF, 
vbak.TAXK1 as Alternativetaxclassification_TAXK1, vbak.TAXK2 as Taxclassification2_TAXK2, vbak.TAXK3 as Taxclassification3_TAXK3, vbak.TAXK4 as TaxClassification4_TAXK4, 
vbak.TAXK5 as Taxclassification5_TAXK5, vbak.TAXK6 as Taxclassification6_TAXK6, vbak.TAXK7 as Taxclassification7_TAXK7, vbak.TAXK8 as Taxclassification8_TAXK8, 
vbak.TAXK9 as Taxclassification9_TAXK9, vbak.XBLNR as ReferenceDocumentNumber_XBLNR, vbak.ZUONR as Assignmentnumber_ZUONR, 
vbak.VGTYP as PreDocCategory_VGTYP, vbak.AUFNR as OrderNumberHdr_AUFNR, vbak.QMNUM as NotificationNo_QMNUM, 
vbak.VBELN_GRP as Mstercontractnumber_VBELN_GRP, vbak.STCEG_L as TaxDestinationCountry_STCEG_L, vbak.LANDTX as Taxdeparturecountry_LANDTX, 
vbak.HANDLE as Internationaluniquekey_HANDLE, vbak.PROLI as DangerousGoodsManagementProfile_PROLI, vbak.CONT_DG as DangerousGoodsFlag_CONT_DG, 
vbak.UPD_TMSTMP as UTCTimeStampL_UPD_TMSTMP,  
 vbap.MATWA as Materialentered_MATWA, 
vbap.PMATN as PricingReferenceMaterial_PMATN, vbap.CHARG as BatchNumber_CHARG, vbap.MATKL as MaterialGroup_MATKL, vbap.ARKTX as ShortText_ARKTX, 
vbap.PSTYV as ItemCategory_PSTYV, vbap.POSAR as ItemType_POSAR, vbap.LFREL as RelevantforDelivery_LFREL, vbap.FKREL as RelevantforBilling_FKREL, 
vbap.UEPOS as BOMItemLevel_UEPOS, vbap.GRPOS as AlternativeForItem_GRPOS, vbap.ABGRU as RejectionReason_ABGRU, vbap.PRODH as ProductHierarchy_PRODH, 
vbap.ZWERT as TargetValue_ZWERT, vbap.ZMENG as TargetQuantityUoM_ZMENG, vbap.ZIEME as TargetquantityUoM_ZIEME, vbap.UMZIZ as BaseTargetConversionFactor_UMZIZ, 
vbap.UMZIN as ConversionFactor_UMZIN, vbap.MEINS as BaseUnitofMeasure_MEINS, vbap.SMENG as Scalequantity_SMENG, vbap.ABLFZ as Roundingquantityfordelivery_ABLFZ, 
vbap.ABDAT as ReconciliationDate_ABDAT, vbap.ABSFZ as AllowedDeviation_ABSFZ, vbap.POSEX as ItemNumberoftheUnderlyingPurchaseOrder_POSEX, vbap.KDMAT as CustomerMaterialNumber_KDMAT, 
vbap.KBVER as AlloweddeviationPercent_KBVER, vbap.KEVER as Daysbywhichthequantitycanbeshifted_KEVER, vbap.VKGRU as Repairprocessing_VKGRU, vbap.VKAUS as UsageIndicator_VKAUS, 
vbap.GRKOR as Deliverygroup_GRKOR, vbap.FMENG as Quantityisfixed_FMENG, vbap.UEBTK as Unlimitedoverdeliveryallowed_UEBTK, vbap.UEBTO as OverdeliveryToleranceLimit_UEBTO, 
vbap.UNTTO as UnderdeliveryToleranceLimit_UNTTO, vbap.FAKSP as Billingblockforitem_FAKSP, vbap.ATPKZ as Replacementpart_ATPKZ, vbap.RKFKF as FormofBillingforCO_RKFKF, 
vbap.SPART as Division_SPART, vbap.GSBER as BusinessArea_GSBER, vbap.NETWR as NetPrice_NETWR, vbap.WAERK as Currency_WAERK, vbap.ANTLF as MaximumPartialDeliveries_ANTLF, 
vbap.KZTLF as Partialdeliveryatitemlevel_KZTLF, vbap.CHSPL as Batchsplitallowed_CHSPL, vbap.KWMENG as CumulativeOrderQuantity_KWMENG, vbap.LSMENG as CumulativeTargetDeliveryQty_LSMENG, 
vbap.KBMENG as CumulativeConfirmedQuantity_KBMENG, vbap.KLMENG as CumulativeConfirmedQuantityinBaseUoM_KLMENG, vbap.VRKME as Salesunit_VRKME, 
vbap.UMVKZ as NumeratorQty_UMVKZ, vbap.UMVKN as DenominatorQty_UMVKN, vbap.BRGEW as Grossweightofitem_BRGEW, vbap.NTGEW as Netweightofitem_NTGEW, 
vbap.GEWEI as WeightUnit_GEWEI, vbap.VOLUM as Volumeoftheitem_VOLUM, vbap.VOLEH as Volumeunit_VOLEH, vbap.VBELV as Originatingdocument_VBELV, 
vbap.POSNV as Originatingitem_POSNV, vbap.VGBEL as ReferenceDocument_VGBEL, vbap.VGPOS as ReferenceItem_VGPOS, vbap.VOREF as ReferenceIndicator_VOREF, 
vbap.UPFLU as UpdateIndicator_UPFLU, vbap.ERLRE as CompletionruleforQuotation_ERLRE, vbap.LPRIO as DeliveryPriority_LPRIO, vbap.WERKS as Plant_WERKS, 
vbap.LGORT as StorageLocation_LGORT, vbap.VSTEL as ShippingReceivingPoint_VSTEL, vbap.ROUTE as Route_ROUTE, vbap.STKEY as BOMOrigin_STKEY, 
vbap.STDAT as BOMDate_STDAT, vbap.STLNR as BOM_STLNR, vbap.AWAHR as Orderprobabilityoftheitem_AWAHR, 
vbap.TAXM1 as Taxclassification1_TAXM1, vbap.TAXM2 as Taxclassification2_TAXM2, vbap.TAXM3 as Taxclassification3_TAXM3, 
vbap.TAXM4 as Taxclassification4_TAXM4, vbap.TAXM5 as Taxclassification5_TAXM5, vbap.TAXM6 as Taxclassification6_TAXM6, vbap.TAXM7 as Taxclassification7_TAXM7, 
vbap.TAXM8 as Taxclassification8_TAXM8, vbap.TAXM9 as Taxclassification9_TAXM9, vbap.VBEAF as Fixedshippingprocessingtimeindays_VBEAF, vbap.VBEAV as Variableshippingprocessingtimeindays_VBEAV, 
vbap.VGREF as Precedingdocumenthasresultedfromreference_VGREF, vbap.NETPR as Netprice_NETPR, vbap.KPEIN as Conditionpricingunit_KPEIN, vbap.KMEIN as ConditionUnit_KMEIN, 
vbap.SHKZG as ReturnsItem_SHKZG, vbap.SKTOF as Cashdiscountindicator_SKTOF, vbap.MTVFP as CheckingGroupforAvailabilityCheck_MTVFP, vbap.SUMBD as Summingupofrequirements_SUMBD, 
vbap.KONDM as MaterialPricingGroup_KONDM, vbap.KTGRM as Accountassignmentgroupforthismaterial_KTGRM, vbap.BONUS as Volumerebategroup_BONUS, vbap.PROVG as Commissiongroup_PROVG, 
vbap.PRSOK as PricingisOK_PRSOK, vbap.BWTAR as Valuationtype_BWTAR, vbap.BWTEX as Separatevaluation_BWTEX, vbap.XCHPF as Batchmanagementrequirementindicator_XCHPF, 
vbap.XCHAR as Batchmanagementindicator_XCHAR, vbap.LFMNG as Minimumdeliveryquantityindeliverynoteprocessing_LFMNG, vbap.STAFO as Updategroupforstatisticsupdate_STAFO, 
vbap.WAVWR as Costindocumentcurrency_WAVWR, vbap.KZWI1 as Subtotal1frompricingprocedureforcondition_KZWI1, vbap.KZWI2 as Subtotal2frompricingprocedureforcondition_KZWI2, 
vbap.KZWI3 as Subtotal3frompricingprocedureforcondition_KZWI3, vbap.KZWI4 as Subtotal4frompricingprocedureforcondition_KZWI4, vbap.KZWI5 as Subtotal5frompricingprocedureforcondition_KZWI5, 
vbap.KZWI6 as Subtotal6frompricingprocedureforcondition_KZWI6, vbap.STCUR as Exchangerateforstatistics_STCUR, vbap.AEDAT as LastChangedOn_AEDAT, vbap.EAN11 as InternationalArticleNumber_EAN11, 
vbap.FIXMG as Deliverydateandquantityfixed_FIXMG, vbap.PRCTR as ProfitCenter_PRCTR, vbap.MVGR1 as Materialgroup1_MVGR1, vbap.MVGR2 as Materialgroup2_MVGR2, 
vbap.MVGR3 as Materialgroup3_MVGR3, vbap.MVGR4 as Materialgroup4_MVGR4, vbap.MVGR5 as Materialgroup5_MVGR5, vbap.KMPMG as ComponentQuantity_KMPMG, vbap.SUGRD as Reasonformaterialsubstitution_SUGRD, 
vbap.SOBKZ as SpecialStockIndicator_SOBKZ, vbap.VPZUO as AllocationIndicator_VPZUO, vbap.PAOBJNR as ProfitabilitySegmentNumber_PAOBJNR, vbap.PS_PSP_PNR as WBSElement_PS_PSP_PNR, vbap.AUFNR as OrderNumber_AUFNR, 
vbap.VPMAT as Planningmaterial_VPMAT, vbap.VPWRK as Planningplant_VPWRK, vbap.PRBME as Baseunitofmeasureforproductgroup_PRBME, vbap.UMREF as Conversionfactorquantities_UMREF, 
vbap.KNTTP as Accountassignmentcategory_KNTTP, vbap.KZVBR as Consumptionposting_KZVBR, vbap.SERNR as BOMexplosionnumber_SERNR, vbap.OBJNR as Objectnumberatitemlevel_OBJNR, 
vbap.ABGRS as ResultsAnalysisKey_ABGRS, vbap.BEDAE as Requirementstype_BEDAE, vbap.CMPRE as Itemcreditprice_CMPRE, vbap.CMTFG as creditblock_CMTFG, vbap.CMPNT as Relevantforcredit_CMPNT, 
vbap.CUOBJ as Configuration_CUOBJ, vbap.CUOBJ_CH as Internalobjectnumberofthebatchclassification_CUOBJ_CH, vbap.CEPOK as Statusexpectedprice_CEPOK, vbap.KOUPD as Conditionupdate_KOUPD, 
vbap.SERAIL as SerialNumberProfile_SERAIL, vbap.ANZSN as Numberofserialnumbers_ANZSN, vbap.NACHL as Customerhasnotpostedgoodsreceipt_NACHL, vbap.MAGRV as PackagingMaterials_MAGRV, 
vbap.MPROK as Statusmanualpricechange_MPROK, vbap.VGTYP as PrecedingDocCategory_VGTYP, vbap.KALNR as CostEstimateNumber_KALNR, vbap.KLVAR as CostingVariant_KLVAR, vbap.SPOSN as BOMItemNumber_SPOSN, 
vbap.KOWRR as Statisticalvalues_KOWRR, vbap.STADAT as Statisticsdate_STADAT, vbap.EXART as BusinessTransactionTypeforForeignTrade_EXART, vbap.PREFE as ImportExportFlag_PREFE, 
vbap.KNUMH as Numberofconditionrecord_KNUMH, vbap.CLINT as InternalClassNumber_CLINT, vbap.STLTY as BOMCategory_STLTY, vbap.STLKN as BOMItemNodeNumber_STLKN, 
vbap.STPOZ as Internalcounter_STPOZ, vbap.STMAN as Inconsistentconfiguration_STMAN, vbap.ZSCHL_K as Overheadkey_ZSCHL_K, vbap.KALSM_K as CostingSheet_KALSM_K, 
vbap.KALVAR as CostingVariant_KALVAR, vbap.KOSCH as Productallocation_KOSCH, vbap.UPMAT as PricingReferenceMaterial_UPMAT, vbap.UKONM as MaterialPricingGroup_UKONM, 
vbap.MFRGR as MaterialFreightGroup_MFRGR, vbap.PLAVO as PlanningReleaseRegulation_PLAVO, vbap.KANNR as KANBAN_KANNR, vbap.CMPRE_FLT as Itemcreditprice_CMPRE_FLT, vbap.ABFOR as Formofpaymentguarantee_ABFOR, 
vbap.ABGES as GuaranteedFactor_ABGES, vbap.WKTNR as Valuecontractno_WKTNR, vbap.WKTPS as Valuecontractitem_WKTPS, vbap.SKOPF as AssortmentModule_SKOPF, vbap.KZBWS as ValuationofSpecialStock_KZBWS, 
vbap.WGRU1 as Materialgrouphierarchy1_WGRU1, vbap.WGRU2 as Materialgrouphierarchy2_WGRU2, vbap.KNUMA_PI as Promotion_KNUMA_PI, vbap.KNUMA_AG as Salesdeal_KNUMA_AG, vbap.KZFME as LeadingUoM_KZFME, 
vbap.LSTANR as FreegoodsDeliveryControl_LSTANR, vbap.TECHS as ParameterVariant_TECHS, vbap.MWSBP as Taxamountindocumentcurrency_MWSBP, vbap.BERID as MRPArea_BERID, 
vbap.PCTRF as ProfitCenterforBilling_PCTRF, vbap.STOCKLOC as ManagingLocation_STOCKLOC, vbap.SLOCTYPE as TypeofFirstInventory_SLOCTYPE, vbap.MSR_RET_REASON as ReturnReason_MSR_RET_REASON, vbap.MSR_REFUND_CODE as ReturnsRefundCode_MSR_REFUND_CODE, 
vbap.MSR_APPROV_BLOCK as ApprovalBlock_MSR_APPROV_BLOCK, vbap.NRAB_KNUMH as Conditionrecordnumber_NRAB_KNUMH, vbap.TRMRISK_RELEVANT as RiskRelevancyinSales_TRMRISK_RELEVANT, 
vbap.HANDOVERLOC as Locationforaphysicalhandoverofgoods_HANDOVERLOC, vbap.HANDOVERDATE as HandoverDateattheHandoverLocation_HANDOVERDATE, vbap.HANDOVERTIME as Handovertimeatthehandoverlocation_HANDOVERTIME, 
vbap.TC_AUT_DET as TaxCodeAutomaticallyDetermined_TC_AUT_DET, vbap.MANUAL_TC_REASON as ManualTaxCodeReason_MANUAL_TC_REASON, vbap.FISCAL_INCENTIVE as TaxIncentiveType_FISCAL_INCENTIVE, 
vbap.FISCAL_INCENTIVE_ID as IncentiveID_FISCAL_INCENTIVE_ID, vbap.SPCSTO as NotaFiscalSpecialCaseforCFOPDetermination_SPCSTO, vbap.KOSTL as CostCenter_KOSTL, vbap.FONDS as Fund_FONDS, 
vbap.FISTL as FundsCenter_FISTL, vbap.FKBER as FunctionalArea_FKBER 
from CDC_PROCESSED.vbak as vbak
join CDC_PROCESSED.vbap as vbap 
on vbak.vbeln = vbap.vbeln 
and vbak.mandt = vbap.mandt" \
SAP_REPORTING.SalesOrders

###

bq mk \
--use_legacy_sql=false \
--view \
'SELECT SO.Client_MANDT, SO.SalesOrder_VBELV, SO.SalesItem_POSNV, so_status.Delivery_Status, SO.DeliveryNumber_VBELV, SO.DeliveryItem_POSNV, SO.InvoiceNumber_VBELN, SO.InvoiceItem_POSNN,
       vbap.CumulativeOrderQuantity_KWMENG as SalesQty, vbap.SalesUnit_VRKME, vbap.NetPrice_NETWR, vbap.Currency_WAERK, 
       SO.DeliveredQty_RFMNG, SO.DeliveredUoM_MEINS, SO.InvoiceQty_RFMNG, SO.InvoiceUoM_MEINS, SO.InvoiceValue_RFWRT, SO.InvoiceCurrency_WAERS,
       vbap.MaterialNumber_MATNR, mat.MaterialText_MAKTX,  vbap.ProductHierarchy_PRODH, mat.Language_SPRAS
FROM SAP_REPORTING.SDDocumentFlow as SO
join SAP_REPORTING.SalesOrders as vbap 
    on SO.Client_MANDT = vbap.Client_MANDT 
    and SO.SalesOrder_VBELV = vbap.SalesDocument_VBELN and SO.SalesItem_POSNV = vbap.Item_POSNR
join SAP_REPORTING.MaterialsMD 
    AS mat ON SO.Client_MANDT = mat.Client_MANDT 
    and vbap.MaterialNumber_MATNR = mat.MaterialNumber_MATNR
join SAP_REPORTING.SDStatus_Items as so_status 
    on SO.Client_MANDT = so_status.Client_MANDT 
    and SO.SalesOrder_VBELV = so_status.SDDocumentNumber_VBELN
    and SO.SalesItem_POSNV = so_status.ItemNumberOfTheSdDocument_POSNR
left outer join SAP_REPORTING.SDStatus_Items as del_status 
    on SO.Client_MANDT = del_status.Client_MANDT 
    and del_status.SDDocumentNumber_VBELN = SO.DeliveryNumber_VBELV
    and del_status.ItemNumberOfTheSdDocument_POSNR = SO.DeliveryItem_POSNV
order by SalesOrder_VBELV, SalesItem_POSNV, SO.DeliveryNumber_VBELV, SO.DeliveryItem_POSNV' \
SAP_REPORTING.SalesStatus_Items


###

bq mk \
--use_legacy_sql=false \
--view \
'with SO AS (
SELECT  SO.Client_MANDT , SUM(SO.InvoiceQty_RFMNG) AS BilledQty, SO.InvoiceUoM_MEINS, 
        SUM(SO.InvoiceValue_RFWRT) AS InvoicePrice, SO.InvoiceCurrency_WAERS,
        SO.MaterialNumber_MATNR, SO.MaterialText_MAKTX,  SUM(SO.DeliveredQty_RFMNG) AS DeliveredQty, 
        SO.DeliveredUoM_MEINS, ( SUM(SO.DeliveredQty_RFMNG) - SUM(SO.InvoiceQty_RFMNG) ) AS DeliveredPendingBilling,
        SO.ProductHierarchy_PRODH, SO.Language_SPRAS
    FROM SAP_REPORTING.SalesStatus_Items AS SO
    GROUP BY SO.Client_MANDT,  Currency_WAERK, DeliveredUoM_MEINS, InvoiceUoM_MEINS, InvoiceCurrency_WAERS,
    SalesUnit_VRKME, MaterialNumber_MATNR, MaterialText_MAKTX,  
    ProductHierarchy_PRODH, Language_SPRAS
)
SELECT SO.Client_MANDT, SO.MaterialNumber_MATNR, SO.MaterialText_MAKTX, SUM(vbap.CumulativeOrderQuantity_KWMENG) AS SalesQty, 
        vbap.SalesUnit_VRKME, SUM(vbap.NetPrice_NETWR) AS NetPrice, vbap.Currency_WAERK,
        SO.DeliveredQty, SO.DeliveredUoM_MEINS, 
        ( SUM(vbap.CumulativeOrderQuantity_KWMENG) - SO.DeliveredQty ) AS PendingDelivery,
        SO.DeliveredPendingBilling,vbap.SalesOrganizatiON_VKORG   
FROM SAP_REPORTING.SalesOrders AS vbap 
LEFT OUTER JOIN SO 
    ON SO.Client_MANDT = vbap.Client_MANDT 
    AND SO.MaterialNumber_MATNR = vbap.MaterialNumber_MATNR 
    AND SO.InvoiceCurrency_WAERS = vbap.Currency_WAERK
GROUP BY SO.Client_MANDT, vbap.SalesUnit_VRKME, vbap.Currency_WAERK, SO.MaterialNumber_MATNR, SO.MaterialText_MAKTX,  SO.DeliveredUoM_MEINS, SO.DeliveredQty, SO.DeliveredUoM_MEINS, SO.DeliveredPendingBilling, SalesOrganizatiON_VKORG' \
SAP_REPORTING.SalesFulfillment

