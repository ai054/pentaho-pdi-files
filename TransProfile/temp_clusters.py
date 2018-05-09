from __future__ import division
import math
import sys
import pandas as pd
import numpy as np
import s3fs
from pyhive import hive
from pyhive.exc import *
from scipy.cluster import hierarchy


REMOTE_HIVE_ADDRESS = '34.239.170.20'
REMOTE_HIVE_PORT = 10000


class Attr_group():

    def boxplot(self, Y):
        # For getting the IQR from box plot and calculation
        Y1 = Y[:, 2]
        q75, q25 = np.percentile(Y1, [75, 25])
        calc = np.median(Y1) + 1.5 * (q75 - q25)
        for i in range(len(Y1)):
            max_d = Y1[i - 1]
            if Y1[i] > calc:
                break

        # Details of the heights of clustering and the height which the cut is calculated
        # pickle.dump([Y1,max_d],open("hierarchy_details.pkl","wb"))
        return 10

    def entropy_calc(self, elem):
        # Function for getting entropy
        lns = elem.sum()
        entrpy = -1 * sum([(count / lns) * math.log(count / lns, 2) for count in elem])

        return entrpy

    def dist_entr(self, a, b):
        # pickle.dump(a,open("dist_a.pkl","wb"))
        # pickle.dump(b,open("dist_b.pkl","wb"))
        # For calculating the entropy gain
        entl = self.entropy_calc(a)
        entr = self.entropy_calc(b)

        elem = np.array([a[ii] + b[ii] for ii in xrange(len(b))])
        el_sum = elem.sum()

        # Old entropy.... Calculating individual entropy
        old_entr = self.entropy_calc(elem)

        # New entropy.... Calculating entropy based on its children
        new_entr = (a.sum() / el_sum) * entl + (b.sum() / el_sum) * entr

        # Calculating entropy gain or information loss
        entrpy_gain = old_entr - new_entr

        return entrpy_gain

    def attr_clustering(self, X, label_in, a):
        # For doing hierarchical clustering
        # generate the linkage matrix

        Z = hierarchy.linkage(X, method='complete', metric=self.dist_entr)

        # Variable used for creating dendrogram
        # pickle.dump(Z,open("hierarchy_Z.pkl","wb"))

        # Code for calculating the number of clusters
        # Here the code will return the max_depth which the information loss is higher than its children
        # max_d = self.boxplot(Z)

        # Flattening the clusters based on distance
        # cls = hierarchy.fcluster(Z,max_d, criterion='distance')

        k = 10
        cls = hierarchy.fcluster(Z, k, criterion='maxclust')
        clusters = np.array([str(i) for i in cls])

        # Dataframe containing entries and corresponding clusters
        df1 = pd.DataFrame({a + "_cid": clusters, a: label_in})

        return df1

    # return X

    def start(self, df2, lam, num):

        # Code starts here
        df_list = {}
        alpha = 1 / (num - 1)

        def add_one(x):
            return (x + lam * alpha) / (x.sum() + lam)

        # dfpivot2 = df2.pivot(index='cluster', columns='tp_id', values='tp_freq')

        label_in = df2[df2.columns[0]]
        df3 = df2[df2.columns[1:]]
        # label_in = df2.index.get_values()
        dfpivot4 = df3.fillna(0)
        dfpivot5 = dfpivot4.apply(add_one, axis=1)
        X = dfpivot5.as_matrix(columns=None)

        df = self.attr_clustering(X, label_in, 'clusters')

        return df


def connect_to_hive(ip=REMOTE_HIVE_ADDRESS, port=REMOTE_HIVE_PORT):
    """ Establish cursor to Hive DB
    :param ip:
    :param port:
    :return: Cursor
    """
    return hive.connect(ip, port).cursor()


def get_table_data_hive(list_of_queries, **kwargs):
    """Gets table data in the form of DataFrame
    :return:
    """
    assert isinstance(list_of_queries, (list, tuple)), 'Queries must be arranged in a list'
    assert len(list_of_queries), "List shouldn't be empty"

    if 'ip' in kwargs and 'port' in kwargs:
        cursor_passed = connect_to_hive(kwargs['ip'], kwargs['port'])
    else:
        cursor_passed = connect_to_hive()
    cursor_passed.execute("set hive.execution.engine=mr")
    cursor_passed.execute("set mapred.input.dir.recursive=true")
    cursor_passed.execute("use {}".format(kwargs['db_name']))

    for query_ in list_of_queries:
        cursor_passed.execute(query_)

    if 'return_' in kwargs and kwargs['return_']:
        res = cursor_passed.description
        description = list(col[0] for col in res)
        headers = [x.split(".")[1] if "." in x else x for x in description]
        try:
            data = pd.DataFrame(cursor_passed.fetchall(), columns=headers)
        except OperationalError:
            print(cursor_passed.fetch_logs())
            exit()
        except ProgrammingError as err:
            print(err.message)
            exit()
        else:
            return data
        finally:
            cursor_passed.close()
    else:
        return True


class S3_configurations:
    key_id = "AKIAI26L75POJOGN2RTQ"
    access_key = "XiMU9Prd+05D+NtLvh+kp3R42zSGdsk14TnPKrzZ"
    bucket = "tatras-octopi"


class PushToS3:
    def __init__(self, out_name, account_id):
        self.bucket = S3_configurations.bucket
        self.folder = "{}_input".format(out_name)
        self.filename = "{}_input_{}".format(out_name, account_id)
        self.key_id = S3_configurations.key_id
        self.access_key = S3_configurations.access_key

    def push_csv_to_s3(self, df):
        """Pushes data to s3
        """

        bytes_to_write = df.to_csv(None).encode()
        fs = s3fs.S3FileSystem(key=self.key_id, secret=self.access_key)
        with fs.open('s3://{}/{}/{}/{}.csv'.format(self.bucket, self.folder, self.filename, self.filename),
                     'wb') as f:
            f.write(bytes_to_write)


def some_function(_df):
    return _df


if __name__ == '__main__':
    REMOTE_HIVE_ADDRESS = sys.argv[1]

    # Provide table name here
    table_name = 'test_clusters'
    df_ = get_table_data_hive(list_of_queries=('select * from {}'.format(table_name),), db_name='feeds', return_=True)

    # run your function over here. This gets you I guess cluster id and attribute
    df__ = some_function(df_)

    # push that dataframe to s3
    PushToS3(out_name="attribute_cluster_id", account_id=2).push_csv_to_s3(df__)

    # Create external table on hive
    get_table_data_hive(("drop table if exists attributes_clusters",
                         "CREATE EXTERNAL TABLE attributes_clusters(index_ string, "
                         "cluster_attribute string,"
                         " cluster_id string)"
                         " ROW format delimited"
                         " FIELDS terminated by ','"
                         " LOCATION 's3n://tatras-octopi/attribute_cluster_id/attribute_cluster_id_2'"
                         " tblproperties ('skip.header.line.count'='1')"),
                        db_name="feeds")
