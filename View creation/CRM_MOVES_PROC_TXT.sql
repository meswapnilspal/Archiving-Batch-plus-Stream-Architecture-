-- Databricks notebook source
CREATE VIEW CRM_MOVES_PROC_TXT  AS SELECT
       process
       ,text
FROM ZCRM_MOVES_PROC