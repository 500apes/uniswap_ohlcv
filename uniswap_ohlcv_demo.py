#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  3 17:55:00 2021
@author: 500apes
"""
import uniswap_ohlcv as ohlcv

# Create the class
gql = ohlcv.graphql_class(verbose=True)

# Load the OHLCV data
gql.get_ohlcv(days=3,timescale="5min",pair="0xa478c2975ab1ea89e8196811f51a7b7ade33eb11")

# Save teh OHLCV data to test.csv
gql.save_csv("test.csv")