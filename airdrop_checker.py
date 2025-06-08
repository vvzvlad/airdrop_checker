#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# flake8: noqa
# pylint: disable=broad-exception-raised, raise-missing-from, too-many-arguments, redefined-outer-name
# pylint: disable=multiple-statements, logging-fstring-interpolation, trailing-whitespace, line-too-long
# pylint: disable=broad-exception-caught, missing-function-docstring, missing-class-docstring
# pylint: disable=f-string-without-interpolation, wrong-import-position
# pylance: disable=reportMissingImports, reportMissingModuleSource

import time
import json
import logging
import sys
import random
import os
import re
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from jsonpath_ng import jsonpath, parse

from grist_api import GristDocAPI
import colorama
import requests


class GRIST:
    def __init__(self, server, doc_id, api_key, nodes_table, settings_table, logger):
        self.server = server
        self.doc_id = doc_id
        self.api_key = api_key
        self.nodes_table = nodes_table.replace(" ", "_")
        self.settings_table = settings_table.replace(" ", "_")
        self.logger = logger
        self.grist = GristDocAPI(doc_id, server=server, api_key=api_key)

    def to_timestamp(self, dtime: datetime) -> int:
        if dtime.tzinfo is None:
            dtime = dtime.replace(tzinfo=timezone(timedelta(hours=3))) 
        return int(dtime.timestamp())

    def update_column(self, row_id, column_name, value, table=None):
        if isinstance(value, datetime):
            value = self.to_timestamp(value)
        column_name = column_name.replace(" ", "_")
        self.grist.update_records(table or self.nodes_table, [{ "id": row_id, column_name: value }])

    def update(self, row_id, updates, table=None):
        for column_name, value in updates.items():
            if isinstance(value, datetime):
                updates[column_name] = self.to_timestamp(value)
        updates = {column_name.replace(" ", "_"): value for column_name, value in updates.items()}
        self.grist.update_records(table or self.nodes_table, [{"id": row_id, **updates}])

    def fetch_table(self, table=None):
        return self.grist.fetch_table(table or self.nodes_table)

    def find_record(self, record_id=None, state=None, name=None, table=None):
        table = self.grist.fetch_table(table or self.nodes_table)
        if record_id is not None:
            record = [row for row in table if row.id == record_id]
            return record
        if state is not None and name is not None:
            record = [row for row in table if row.State == state and row.name == name]
            return record
        if state is not None:
            record = [row for row in table if row.State == state]
            return record
        if name is not None:
            record = [row for row in table if row.Name == name]
            return record

    def find_settings(self, setting, table=None):
        if table is None: table = self.settings_table
        else: table = table.replace(" ", "_")
        data = self.grist.fetch_table(table)
        if setting is None:
            raise ValueError("Setting name is not provided")
        if setting not in [row.Setting for row in data]:
            raise ValueError(f"Setting {setting} not found in table {self.settings_table}")
        value = [row for row in data if row.Setting == setting][0].Value
        if value == "" or value is None:
            raise ValueError(f"Setting {setting} is empty")
        return value

    #def find_settings(self, setting):
    #    data = getattr(self.fetch_table(self.settings_table)[0], setting)
    #    return data

    def find_chain(self, target_id, table):
        if target_id is None or target_id == "" or int(target_id) == 0:
            raise Exception("Chain is None!")
        data = self.grist.fetch_table(table)
        if len(data) == 0:
            raise Exception("Chains table is empty!")
        search_result = [row for row in data if row.id == target_id]
        if len(search_result) == 0:
            raise Exception(f"Chain not found!")
        api = search_result[0].API
        if api is None or api == "":
            raise Exception(f"API is None!")
        return api

    def nodes_table_preprocessing(self):
        current_time = self.to_timestamp(datetime.now())
        max_wip = 60*60*2

        self.logger.info(f"Run grist processing NoneState -> Dirty and NoneVersion -> av1")
        for row in self.fetch_table():
            if row.State == "": self.update_column(row.id, "State", "Dirty")
            if row.Version == "": self.update_column(row.id, "Version", "av1")

        self.logger.info(f"Run grist processing av1 and !WiP -> Dirty")
        for row in self.fetch_table():
            if row.Version == "av1" and row.State != "WiP" and row.State != "Dirty" and row.State != "Error": 
                self.update_column(row.id, "State", "Dirty")
                self.update_column(row.id, "Status", "Set Dirty by old Version")
                
        self.logger.info(f"Run grist processing WiP and >1d old -> Dirty")
        for row in self.fetch_table():
            if row.Deploy_date is not None:
                vm_old_age = current_time - row.Deploy_date
                if row.State == "WiP" and vm_old_age > max_wip and row.State != "Dirty":
                    self.update_column(row.id, "State", "Dirty")
                    self.update_column(row.id, "Status", "Set Dirty by WiP Timeout")

        self.logger.info(f"Run grist processing NoneRetries -> 0/4")
        for row in self.fetch_table():
            if row.Retries is None or row.Retries == "":
                self.update_column(row.id, "Retries", "0/4")



def get_value_by_jsonpath(json_data, json_path):
    jsonpath_expr = parse(json_path)
    match = jsonpath_expr.find(json_data)
    return [m.value for m in match]

def parse_and_sum_jsonpaths(expression, json_data, logger):
    json_paths = expression.split('+')
    total_sum = 0
    missing_paths = []

    for json_path in json_paths:
        json_path = json_path.strip()
        result = get_value_by_jsonpath(json_data, json_path)

        if result:
            logger.info(f"Found value at {json_path}: {result[0]}")
            total_sum += float(result[0])
        else:
            logger.error(f"No value found for path {json_path}")
            missing_paths.append(json_path)
    
    if missing_paths:
        return total_sum, f"Some paths were not found: {', '.join(missing_paths)}"
    else:
        return total_sum, ""
    
def generate_proxy():
    random_token = str(uuid.uuid4())[:10]
    proxy = f"socks5://IDrqLdjR7F3Mlaws0Y2C_s_{random_token}:RNW78Fm5@185.162.130.86:10718"
    return proxy

def check_balance(address, logger, proxy=None):
    hype_price_url = "https://purrfolio.com/api/hype-price"
    debank_url = "https://purrfolio.com/api/debank-data?address="
    hypercore_url = "https://purrfolio.com/api/hypercore-holdings?address="

    proxies = None
    if proxy: 
        proxies = {'http': proxy, 'https': proxy}

    try:
        hype_price_response = requests.get(hype_price_url, proxies=proxies)
        hype_price = float(re.sub(r'[^\d.]', '', str(hype_price_response.json()["price"])))

        debank_response = requests.get(debank_url + address, proxies=proxies)
        debank_usd_value = float(re.sub(r'[^\d.]', '', str(debank_response.json()["usd_value"])))

        hypercore_response = requests.get(hypercore_url + address, proxies=proxies)
        hypercore_usd_value = float(re.sub(r'[^\d.]', '', str(hypercore_response.json()["grandTotal"])))

        hypercore_hype_value = hypercore_usd_value / hype_price
        hyperevm_hype_value = debank_usd_value / hype_price

        return hypercore_hype_value, hyperevm_hype_value

    except Exception as e:
        logger.error(f"Error while checking token transactions for address {address}: {e}")
        raise Exception(f"Error while checking token transactions for address {address}: {e}") from e

def find_none_value(grist, table=None, do_random=False):
    wallets = grist.fetch_table(table)
    if do_random:
        random.shuffle(wallets)
    for wallet in wallets:
        if (wallet.Value is None or wallet.Value == "" ):
            if (wallet.Address is not None and wallet.Address != ""):
                return wallet
    return None
    
def find_none_values(grist, table=None, do_random=False, count=1):
    wallets = grist.fetch_table(table)
    if do_random: random.shuffle(wallets)
    wallets_non_empty_address = [wallet for wallet in wallets if (wallet.Address is not None and wallet.Address != "")]
    wallets_none_value = [wallet for wallet in wallets_non_empty_address if (wallet.Value is None or wallet.Value == "")]
    if do_random: random.shuffle(wallets_none_value)
    return wallets_none_value[:count]

def main():
    colorama.init(autoreset=True)
    logger = logging.getLogger("Token checker")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    server = os.getenv("GRIST_SERVER")
    doc_id = os.getenv("GRIST_DOC_ID")
    api_key = os.getenv("GRIST_API_KEY")
    nodes_table = "Wallets"
    settings_table = "Settings"
    grist = GRIST(server, doc_id, api_key, nodes_table, settings_table, logger)
    while True:
        try:
            #url = grist.find_settings("URL")
            #path = grist.find_settings("Path")
            #logger.info(f"Chain: {url} / {path}")
            #none_value_wallet = find_none_value(grist, do_random=True)
            random.seed(datetime.now().timestamp())
            wallets_count = random.randint(2, 5)
            wallets = find_none_values(grist, do_random=True, count=wallets_count)
            try:
                proxy = generate_proxy()
                if wallets is None or len(wallets) == 0:
                    logger.info("No wallets to check, sleep 10s")
                    time.sleep(10)
                    continue
                for wallet in wallets:
                    try:
                        logger.info(f"Check wallet {wallet.Address} with proxy {proxy}...")
                        hypercore_hype_value, hyperevm_hype_value = check_balance(wallet.Address, logger, proxy)
                        grist.update(wallet.id, {"hypercore_hype_value": hypercore_hype_value, "hyperevm_hype_value": hyperevm_hype_value})  
                    except Exception as e:
                        logger.error(f"Error occurred: {e}")
                        grist.update(wallet.id, {"Value": "--", "Comment": f"Error: {e}"})  
            except Exception as e:
                logger.error(f"Error occurred: {e}")
                logger.error(f"Fail: {e}\n{traceback.format_exc()}")
                time.sleep(10)
                continue

            time.sleep(random.uniform(5*60, 10*60))
        except Exception as e:
            logger.error(f"Error occurred, sleep 10s: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
