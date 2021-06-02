#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
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
     
    def __init__(self):
        
        self.url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
        self.ohlcv = pd.DataFrame()
        self._volume = pd.DataFrame()
        self._price = pd.DataFrame()
        
        
        
    def get_pair_id(self,token0,toekn1):
        pass
#        query whereClause{
#  exchanges(where: {tokenSymbol:"WBTC"}) {
#    id
#    price
#    priceUSD
#    tokenName
#    ethBalance
#    tokenAddress
#    tokenBalance
#    ethLiquidity
#    tokenLiquidity
#    tokenSymbol
#    tradeVolumeEth
#  }
#}
#     
#
#
#  pairs(first: 10, where: {reserveUSD_gt: "1000000", volumeUSD_gt: "50000"}, orderBy: reserveUSD, orderDirection: desc) {
#    id
#    token0 {
#      id
#      symbol
#    }
#    token1 {
#      id
#      symbol
#    }
#    reserveUSD
#    volumeUSD
#  }
#}

   
    def get_active_pairs(self):
        
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

    def _load_csv(self,filename):
        
        try:
            self.ohlcv = pd.read_csv(filename)       
        except:
            print("Bad csv history load... new file? ",sys.exc_info()[0],"occured.")
        
    def save_csv(self,filename="test.csv"):
    
        try:
            with open(filename, 'w') as fn:
                self.ohlcv.to_csv(fn, header=True,index=False)            
        except:
            print("Bad csv write: ",sys.exc_info()[0],"occured.")

        #print(self.ohlcv)
        
        
#        if filename != None:
#            self.load_ohlcv_csv(filename)
#            
#            if len(self.ohlcv) > 0:
#                first_csv_timestamp = self.ohlcv['date'].iloc[0]
#                last_csv_timestamp = self.ohlcv['date'].iloc[-1]
#
#                self._print_daterange()
#    
#                if start_timestamp < first_csv_timestamp:
#                    self.ohlcv = pd.DataFrame()
#                else:
#                    start_timestamp = last_csv_timestamp
        
#        df.resample('1H').agg({'open': 'first', 
#                                 'high': 'max', 
#                                 'low': 'min', 
#                                 'close': 'last',
#                                 'volume': 'sum'})
#        
        
        
    def _print_daterange(self):
        
        first_csv_timestamp = self.ohlcv['date'].iloc[0]
        last_csv_timestamp = self.ohlcv['date'].iloc[-1]

        print("First time in series:",pd.to_datetime(first_csv_timestamp, unit='s'))
        print("Last time in series:",pd.to_datetime(last_csv_timestamp, unit='s'))
        
    def _download_price(self,days=5):
        
        start_timestamp = int(time.time()) - (days * 24 * 3600)
        
        while True:
            query = """query {
                         swaps(first: 1000 where: { timestamp_gt:"""+str(start_timestamp)+""" ,pair: "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11" } orderBy: timestamp, orderDirection: asc) {
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
              #  print("bad data:",json_data)
                time.sleep(10)
                continue
            
            df = pd.DataFrame(df_data)
            try:
                df["price1"] = df["amount0In"].astype(float) / df["amount1Out"].astype(float)
                df["price2"] = df["amount0Out"].astype(float) / df["amount1In"].astype(float)
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
        
    def get_ohlcv(self,days=5,timescale='5min'):
  
        gql._download_price(days=3)

        df3 = self._price.resample(timescale,  axis=0).ohlc().ffill()
        df4 = self._volume.resample(timescale).agg({"volume":'sum'})
        
        ohlcv = pd.concat([df3,df4],axis=1)
        ohlcv = ohlcv.reset_index() 
       
        ohlcv.columns = ['date','open','high','low','close', 'volume']
        ohlcv['date'] = ohlcv['date'].astype(int)/(1e9)
        ohlcv['date'] = ohlcv['date'].astype(int)
  
        self.ohlcv = ohlcv
       # self.ohlcv = self.ohlcv[~self.ohlcv.index.duplicated()]

gql = graphql_class()

gql.get_ohlcv(days=3,timescale="5min")
gql._print_daterange()
gql.save_csv("test.csv")






