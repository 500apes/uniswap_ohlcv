#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UniGraph trade data to OHLCV converter
Created on Sat May 29 19:50:16 2021
@author: 500apes
"""
import requests
import json
import time
import sys
import pandas as pd

class graphql_class:
 
    def __repr__(self):
         return "unigraph()"
     
    def __str__(self):
         return "Uniswap graphql interface"
     
    def __init__(self,verbose=False):
        
        self.url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
        self.ohlcv = pd.DataFrame()
        self._volume = pd.DataFrame()
        self._price = pd.DataFrame()
        self._verbose = verbose
        
    def get_pair_id(self,token0,token1):
        """
        Add functionalty later
        """
        pass
   
    def get_active_pairs(self):
        """
        Add functionalty later
        """
        
        query = """query {
                     pairs(first: 1000, orderBy: reserveUSD, orderDirection: asc) {
                       id,
                       reserveUSD
                    }
                }"""

        r = requests.post(self.url, json={'query': query})
        print(r.status_code)
        print(r.text)
        json_data = json.loads(r.text)
        
        return json_data

    def _load_csv(self,filename):
        """
        Add functionalty later
        """
        try:
            self.ohlcv = pd.read_csv(filename)       
        except:
            print("Bad csv history load... new file? ",sys.exc_info()[0],"occured.")
        
    def save_csv(self,filename="test.csv"):
        """
        Save the OHLCV csv data to local file filename
        """
        try:
            with open(filename, 'w') as fn:
                self.ohlcv.to_csv(fn, header=True,index=False)            
        except:
            print("Bad csv write: ",sys.exc_info()[0],"occured.")

        
    def _print_daterange(self):
        """
        Print the OHLCV csv data date range
        """
        first_csv_timestamp = self.ohlcv['date'].iloc[0]
        last_csv_timestamp = self.ohlcv['date'].iloc[-1]

        print("First time in series:",pd.to_datetime(first_csv_timestamp, unit='s'))
        print("Last time in series:",pd.to_datetime(last_csv_timestamp, unit='s'))
        
    def _download_price(self,days=5,pair="0xa478c2975ab1ea89e8196811f51a7b7ade33eb11"):
        """
        Default download 5 days of trades of ETH/USD(?) prices from Uniswap Graph
        """
        
        start_timestamp = int(time.time()) - (days * 24 * 3600)
        
        while True:
            query = """query {
                         swaps(first: 1000 where: { timestamp_gt:"""+str(start_timestamp)+""" ,pair: """+str(pair)+""" } orderBy: timestamp, orderDirection: asc) {
                          transaction {
                            id
                            timestamp
                          }
                          id
                          amount0In
                          amount0Out
                          amount1In
                          amount1Out
                          amountUSD
                          timestamp
                          to
                        } 
                    }"""
                         
            r = requests.post(self.url, json={'query': query})
            json_data = json.loads(r.text)
            
            try:
                df_data = json_data['data']['swaps']
            except:
                print("Bad data.. sleep and try again")
                print("bad data:",json_data)
                time.sleep(10)
                continue
            
            df = pd.DataFrame(df_data)
            try:
                df["price1"] = df["amount0In"].astype(float) / df["amount1Out"].astype(float) #--Buy side
                df["price2"] = df["amount0Out"].astype(float) / df["amount1In"].astype(float) #--Sell side
                df['price'] = df['price1'].combine_first(df['price2'])

                df["volume"] = df["amountUSD"].astype(float)
            except:
                print("Broke or finished")
                break

            start_timestamp = max(df['timestamp'].astype(int))
            df['timestamp2']  = pd.to_datetime(df['timestamp'], unit='s')
             
            df_price = df[['timestamp2','price']]
            df_price = df_price.set_index('timestamp2')
          
            df_vol = df[['timestamp2','volume']]
            df_vol = df_vol.set_index('timestamp2')
            
            self._volume = self._volume.append(df_vol)
            self._price = self._price.append(df_price)
        
    def get_ohlcv(self,days=5,timescale='5min',pair="0xa478c2975ab1ea89e8196811f51a7b7ade33eb11"):
  
        self._download_price(days) #---scrape raw price data from Uniswap Graph

        df3 = self._price.resample(timescale,  axis=0).ohlc().ffill()
        df4 = self._volume.resample(timescale).agg({"volume":'sum'})
        
        ohlcv = pd.concat([df3,df4],axis=1)
        ohlcv = ohlcv.reset_index() 
       
        ohlcv.columns = ['date','open','high','low','close', 'volume']
        ohlcv['date'] = ohlcv['date'].astype(int)/(1e9)
        ohlcv['date'] = ohlcv['date'].astype(int)
  
        self.ohlcv = ohlcv
       # self.ohlcv = self.ohlcv[~self.ohlcv.index.duplicated()]
       
        if self._verbose:
           self._print_daterange()







