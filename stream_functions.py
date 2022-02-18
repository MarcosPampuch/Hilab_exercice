# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 19:40:24 2022

@author: User
"""

import requests
import os
import json
import sys
import pandas as pd
sys.path.append("/home/marcos/project_Hilab")
#from keys import api_key, api_secret_key, bearer_token

# To set your enviornment variables in your terminal run the following line:
#export 'BEARER_TOKEN'='AAAAAAAAAAAAAAAAAAAAAD9pZQEAAAAAltf9rkm8GWMJ52SHf5tjRk8rKRo%3Ds0aQ4HxEkj3it3edkr5v6Q2k1LkTFj2zA0D6fQetzCHz9cNNDh'
#bearer_token= os.environ.get("BEARER_TOKEN")
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAD9pZQEAAAAAltf9rkm8GWMJ52SHf5tjRk8rKRo%3Ds0aQ4HxEkj3it3edkr5v6Q2k1LkTFj2zA0D6fQetzCHz9cNNDh'
#print(bearer_token)

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    print("\n#############FUNC GET RULES#########\n")
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))
    print("\n#############FUNC DELETE RULES#########\n")


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "Futebol lang:pt", "tag": "Soccer rule"},
        {"value": "Saúde lang:pt", "tag": "Health rule"},
        {"value": "Comida lang:pt", "tag": "Food rule"}
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    print("\n#############FUNC SET RULES#########\n")


def get_stream(set):
    response = requests.get(
#        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at", 
        auth=bearer_oauth, stream=True)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
#    with open('sample.json', 'w', encoding='utf-8') as f:        
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
#            print(json.dumps(json_response, indent=4, sort_keys=True))
            ### Creating Dataframe with data
            df_nested = pd.json_normalize(json_response, record_path =['matching_rules'], meta=[['data','text'], ['data','id'],['data', 'created_at']])
#            print(df_nested_list)
#            print(json.dumps(json_response, indent=4, sort_keys=True))
#                result = json.dumps(json_response, indent=4, sort_keys=True)
#                json.dump(result, f, ensure_ascii=False)
            ## Creating list with future values of columns DATE and HOUR
            date_hour = df_nested.loc[0, 'data.created_at']
            date_hour = date_hour.split(sep='T')
            date_hour[-1] = date_hour[-1].split(sep='.')[0]
            df_nested.loc[0,'data.created_at'] = date_hour[0]
            df_nested['hour'] = date_hour[-1]
            df_nested = df_nested[['data.id','data.text','tag', 'data.created_at','hour']]
    

def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set)


if __name__ == "__main__":
    main()