-- Databricks notebook source
SELECT p.RefExternal,cal.DayDate,p.Uplift FROM (
    SELECT DISTINCT  c.FullName, 
            p.FullName AS ProductName,
            p.ProductId,
            pmc.RefExternal,
            sis.VariantText AS PromotionName,
            pds.DayDate AS PromotionStart,
            pde.DayDate AS PromotionEnd,
            siss.FullName AS PromotionType,
            (CASE WHEN ISNULL(psism.PercentageIncrease) THEN 0 ELSE psism.PercentageIncrease END) / 100 AS Uplift
                       FROM BOBv2.Product p
        INNER JOIN BOBv2.ProductMapChain pmc                                ON p.ProductId = pmc.ProductId
        INNER JOIN BOBv2.Chain c                                                   ON pmc.ChainId = c.ChainId
         LEFT JOIN BOBv2.Chain pc                                                  ON c.ParentChainId = pc.ChainId
        INNER JOIN BOBv2.ProductSurveyItemSubjectMap psism     ON p.ProductId = psism.ProductId
        INNER JOIN BOBv2.SurveyItemSubject sis                       ON psism.SurveyItemSubjectId = sis.SurveyItemSubjectId
        INNER JOIN BOBv2.SurveyItemSubjectSetMap sissm         ON sis.SurveyItemSubjectId = sissm.SurveyItemSubjectId
        INNER JOIN BOBv2.SurveyItemSubjectSet siss                   ON sissm.SurveyItemSubjectSetId = siss.SurveyItemSubjectSetId
        INNER JOIN BOBv2.SurveyItemMapChain simc                     ON sissm.SurveyItemSubjectSetMapId = simc.SurveyItemSubjectSetMapId
                                                                                                                 AND COALESCE(pc.ChainId, c.ChainId) = simc.ChainId
        INNER JOIN BOBv2.PeriodDay pds                                      ON sissm.PeriodStartDayId = pds.PeriodDayId
         LEFT JOIN BOBv2.PeriodDay pde                                             ON sissm.PeriodEndDayId = pde.PeriodDayId
          WHERE COALESCE(pc.ChainId, c.ChainId) = 23 
                  AND p.IsActive = 1
            AND pmc.Active = 1
            AND sissm.IsActive = 1
            AND siss.IsActive = 1
            AND siss.IsPromotionSet = 1 
                     AND pds.DayDate > '2019-11-11' ) p
CROSS JOIN BOBv2.PeriodDay cal 
WHERE cal.DayDate BETWEEN p.PromotionStart AND p.PromotionEnd


-- COMMAND ----------


