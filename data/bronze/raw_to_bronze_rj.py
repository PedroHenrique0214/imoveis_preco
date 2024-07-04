# Databricks notebook source
# DBTITLE 1,Trazendo do raw para bronze dados RJ
# Vamos ver se o dataset tá no arquivo certo
dbutils.fs.ls("/Volumes/raw/dados_sp/dados_rj/")
