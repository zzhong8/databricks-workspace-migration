-- Databricks notebook source
SELECT TOP (1000) 
[CompanyId],
[Company],
[ProductBrandId],
[ProductBrand],
[Lkp_productGroupId],
[ProductGroupName],
[ParentId],
[ManufacturerId],
[Manufacturer],
[CapType],
[CapValue]
FROM 
[dbo].[vw_BOBv2_Caps]
