import boto3
import os
import pandas as pd
import uuid
from gremlin_python.structure.graph import Graph
from gremlin_python.process.traversal import T
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import __


def lambda_handler(event, context):
    # Input
    dynamodb_table = os.getenv('dynamodb_table')

    # SDK settings:
    dynamodb_resource = boto3.resource('dynamodb')
    ddb_table = dynamodb_resource.Table(dynamodb_table)

    ##################################################################################################
    #   Begin: DDB Extraction
    ##################################################################################################

    # DynamoDB Scan step:
    ddb_response = ddb_table.scan()
    items = ddb_response['Items']

    # Fetch data into DF
    pd_data = []
    for data_row in items:
        pd_data.append(data_row)
        # break

    ##################################################################################################
    #   Begin: All products as Vertices ETL
    ##################################################################################################

    # Drop useless columns
    cols_to_drop = ['sk', 'url', 'aliases']
    df_products = pd.DataFrame(pd_data)
    df_products.drop(cols_to_drop, inplace=True, axis=1)

    ##################################################################################################
    #   Begin: Generate Unique Categories and Styles, as Labels to improve Access Patterns
    ##################################################################################################

    # Create DF for categories, including a UUID for each one:
    df_categories = df_products[['category']].drop_duplicates(subset=['category'])
    df_categories['category_id'] = [uuid.uuid4() for _ in range(len(df_categories.index))]

    # Create DF for styles, including a UUID for each one:
    df_styles = df_products[['style']].drop_duplicates(subset=['style'])
    df_styles['style_id'] = [uuid.uuid4() for _ in range(len(df_styles.index))]

    ##################################################################################################
    #   Begin: Ingest into Neptune
    ##################################################################################################

    # Neptune init:
    graph = Graph()
    remote_conn = DriverRemoteConnection(
        'wss://' + os.getenv('neptune_endpoint') + ':8182/gremlin',
        'g')
    g = graph.traversal().withRemote(remote_conn)

    # Step 1: Insert all products
    for index, row in df_products.iterrows():
        # Insert item by item.
        vertex_insert = g.addV('product') \
            .property(T.id, row['id']) \
            .property('product_name', row['name']) \
            .property('current_stock', row['current_stock']) \
            .property('style', row['style']) \
            .property('gender_affinity', row['gender_affinity']) \
            .property('image', row['image']) \
            .property('category', row['category']) \
            .property('description', row['description']) \
            .property('price', row['price']) \
            .property('featured', row['featured']) \
            .next()

        # Need performance improvements and optimizations
        # - Updating items, to add Labels array (Neptune does not support dict/maps in addV).
        for prop_label in row['image_labels']:
            if prop_label['confidence'] > 75:
                update_results = g.V(vertex_insert).property('labels_confidence_gt_75',
                                                             prop_label['name'].lower()).next()

    # Step 2: Insert all categories, adding these as multi-label vertices to improve searchability
    for index, row in df_categories.iterrows():
        g.addV('category::{}'.format(row['category'])).property(T.id, str(row['category_id'])).property(
            'name', row['category']).next()

    # Step 3: Insert all styles, adding these as multi-label vertices to improve searchability
    for index, row in df_styles.iterrows():
        g.addV('style::{}'.format(row['style'])).property(T.id, str(row['style_id'])).property(
            'name', row['style']).next()

    ##################################################################################################
    #   Begin: Edges construction, to connect graph vertices
    #          Category -> Style -> Product
    ##################################################################################################

    # Add Category and Style IDs
    df_with_category_ids = pd.merge(df_products, df_categories, on='category', how='inner')
    df_with_cat_and_style_ids = pd.merge(df_with_category_ids, df_styles, on='style', how='inner')

    ##################################################################################################
    # Creating Edges for Categories -> Styles
    ##################################################################################################

    # Create Edges for Categories -> Styles
    df_edges_category_style = df_with_cat_and_style_ids[['category_id', 'style_id']].drop_duplicates(
        subset=['category_id', 'style_id'])

    # Add edges for Categories -> Styles:
    for index, row in df_edges_category_style.iterrows():
        cat_to_style_edge_insert = g.V(str(row['category_id'])).addE('has').to(__.V(str(row['style_id']))).next()
        print(cat_to_style_edge_insert)

    ##################################################################################################
    # Creating Edges for Styles -> Products
    ##################################################################################################

    # Create Edges for Styles --> Products
    df_edges_styles_products = df_with_cat_and_style_ids[['style_id', 'id']].drop_duplicates(subset=['style_id', 'id'])

    # Add edges for Styles --> Products (ID is the original column of a product_id):
    for index, row in df_edges_styles_products.iterrows():
        style_to_prod_edge_insert = g.V(str(row['style_id'])).addE('has').to(__.V(str(row['id']))).next()
        print(style_to_prod_edge_insert)

    # Close connection
    remote_conn.close()

