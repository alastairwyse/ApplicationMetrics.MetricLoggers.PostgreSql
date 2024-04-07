--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Drop Functions / Stored Procedures
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
DROP PROCEDURE InsertIntervalMetrics;
DROP PROCEDURE InsertStatusMetrics;
DROP PROCEDURE InsertAmountMetrics;
DROP PROCEDURE InsertCountMetrics;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Drop Views
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

DROP VIEW public.AllMetricInstancesView;
DROP VIEW public.IntervalMetricInstancesView;
DROP VIEW public.StatusMetricInstancesView;
DROP VIEW public.AmountMetricInstancesView;
DROP VIEW public.CountMetricInstancesView;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Drop Tables
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

DROP TABLE public.SchemaVersions;
DROP TABLE public.IntervalMetricInstances;
DROP TABLE public.StatusMetricInstances;
DROP TABLE public.AmountMetricInstances;
DROP TABLE public.CountMetricInstances;
DROP TABLE public.IntervalMetrics;
DROP TABLE public.StatusMetrics;
DROP TABLE public.AmountMetrics;
DROP TABLE public.CountMetrics;
DROP TABLE public.Categories;