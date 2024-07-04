# Databricks notebook source
import delta

bronze = delta.DeltaTable.forName("spark", "bronze.dados_sp.sp_vendas")
