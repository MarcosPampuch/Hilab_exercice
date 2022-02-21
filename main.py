#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 23:34:03 2022

@author: marcos
"""

import sys
sys.path.append("/home/marcos/project_Hilab")
from stream_sql import StreamSQL

database = 'Tweets'
host = '192.168.15.66'
username = 'm_remote'
password = 'Pampuch1'

def main():
    stream = StreamSQL()
    rules = stream.get_rules_stream()
    delete = stream.delete_rules_stream(rules)
    set = stream.set_rules_stream(delete)
    stream.stream_sql(set, database, host, username, password)


if __name__ == "__main__":
    main()
                
                
                
                
                