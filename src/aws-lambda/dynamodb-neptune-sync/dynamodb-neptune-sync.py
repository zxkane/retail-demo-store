import pandas as pd
import os
from gremlin_python.structure.graph import Graph
from gremlin_python.process.traversal import T
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection


def drop_vertex(g, vertex_id):
    g.V(vertex_id).drop()


def update_vertex(g, row):
    """
    Update all properties to match DynamoDB record
    """
    g.V(row['id']) \
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
    update_image_labels(g, row['id'], row['image_labels'])


def add_vertex(g, row):
    """
    Create new vertex to match DynamoDB record
    """
    g.addV('product') \
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
    update_image_labels(g, row['id'], row['image_labels'])


def update_image_labels(g, vertex_id, image_labels):
    # Only add labels with confidence over 75% to vertex properties
    for prop_label in image_labels:
        if prop_label['confidence'] > 75:
            g.V(vertex_id).property('labels_confidence_gt_75', prop_label['name'].lower()).next()


def format_row(row):
    res = pd.DataFrame(row)
    # Drop unused columns
    res.drop(['sk', 'url', 'aliases'], inplace=True, axis=1)
    return res


def lambda_handler(event, context):
    # Neptune init:
    graph = Graph()
    c = DriverRemoteConnection(
        'wss://' + os.getenv('neptune_endpoint') + ':8182/gremlin',
        'g')
    g = graph.traversal().withRemote(c)

    for record in event.Records:
        if record.eventName == 'INSERT':
            add_vertex(g, format_row(record.dynamodb))
        elif record.eventName == 'MODIFY':
            update_vertex(g, format_row(record.dynamodb))
        elif record.eventName == 'REMOVE':
            drop_vertex(g, record.dynamodb.id)

    # Close connection
    c.close()
