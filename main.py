#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 23:34:03 2022

@author: marcos
"""

import sys
from credentials import credentials, API_token 
from stream_sql import StreamSQL

def main():
    stream = StreamSQL(API_token)
    rules = stream.get_rules_stream()
    delete = stream.delete_rules_stream(rules)
    set = stream.set_rules_stream(delete)
    stream.stream_sql(set)

if __name__ == "__main__":
    main()
                
                
                
                
                