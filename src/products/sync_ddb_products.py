# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

'''
Utility script that can be run locally to reload the products.yaml file from the catalog in DynamoDB.

By default the script will load products from the default location in the repo.
That is, the corresponding YAML files in the src/products-service/data/ directory.
You can override the file location on the command-line.

Usage:

python sync_ddb_products.py --products-table-name PRODUCTS_TABLE_NAME [--products_file PRODUCTS_FILE]

Where:
PRODUCTS_TABLE_NAME is the DynamoDB table name for products
PRODUCTS_FILE (optional) is the location on your local machine where the products.yaml is located (defaults to src/products-service/data/products.yaml)

Your AWS credentials are discovered from your current environment.
'''

import yaml
import boto3
import sys
import getopt
from decimal import Decimal


def product_dict(ddb_item):
    """
    Builds a dictionary from a product's DynamoDB entry
    """
    item = {}
    # Convert fields to the correct data types
    for field, value in ddb_item.items():
        if isinstance(value, list) and field == 'image_labels':
            labels_conv = []
            for label in value:
                labels_conv.append({'confidence': float(label['confidence']), 'name': label['name']})
            item[field] = labels_conv
        elif isinstance(value, str):
            item[field] = value
        elif isinstance(value, Decimal):
            if value == int(value):
                item[field] = int(value)
            else:
                item[field] = float(value)
    return item


if __name__ == "__main__":
    products_file = "src/products-service/data/products.yaml"
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['products-table-name=', 'products-file='])
    except getopt.GetoptError:
        print('Usage: {sys.argv[0]} --products-table-name PRODUCTS_TABLE_NAME [--products_file PRODUCTS_FILE]')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('Usage: {sys.argv[0]} --products-table-name PRODUCTS_TABLE_NAME[--products_file PRODUCTS_FILE]')
            sys.exit()
        elif opt in ('--products-table-name'):
            products_table_name = arg
        elif opt in ('--products-file-name'):
            products_file = arg

    dynamodb_resource = boto3.resource('dynamodb')
    ddb_table = dynamodb_resource.Table(products_table_name)
    products = ddb_table.scan()

    new_yaml = []
    for product in products['Items']:
        new_yaml.append(product_dict(product))

    with open(products_file, "w") as f:
        yaml.dump(new_yaml, f)

    print(f'Products updated: {len(new_yaml)}')
