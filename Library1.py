import pandas as pd
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, MinMaxScaler
from pyspark.ml.feature import VectorAssembler, StringIndexer, MinMaxScaler as PySparkMinMaxScaler
from pyspark.sql import DataFrame as PySparkDataFrame

class Data_utils:
    def __init__(self):
        pass

    def one_hot_pandas(self, dataframe, column_name):
        df_column = dataframe[column_name].values
        onehot_encoder = OneHotEncoder(sparse_output=False)
        onehot_list = onehot_encoder.fit_transform(df_column.reshape(-1, 1)).tolist()
        new_dataframe = pd.DataFrame(onehot_list, columns=onehot_encoder.get_feature_names_out([column_name]))
        dataframe = pd.concat([dataframe.drop(columns=[column_name]), new_dataframe], axis=1)
        return dataframe

    def ordinal_encoder(self, dataframe, column_name):
        list_category = dataframe[column_name].unique()
        ordinal_encoder = OrdinalEncoder(categories=[list_category])
        ordinal_list = ordinal_encoder.fit_transform(dataframe[column_name].values.reshape(-1, 1)).tolist()
        for index in range(len(ordinal_list)):
            ordinal_list[index] = ordinal_list[index][0]
        return ordinal_list

    def min_max_scaler_pandas(self, dataframe, columns_name):
        scaler = MinMaxScaler()
        for column_name in columns_name:
            data_list = scaler.fit_transform(dataframe[column_name].values.reshape(-1, 1)).tolist()
            for index in range(len(data_list)):
                data_list[index] = data_list[index][0]
            dataframe[column_name] = data_list
        return dataframe

    def ordinal_encoder_pyspark(self, dataframe, list_column_names):
        for column in list_column_names:
            indexer = StringIndexer(inputCol=column, outputCol=column + "_index")
            dataframe = indexer.fit(dataframe).transform(dataframe)
        return dataframe

    def min_max_scaler_pyspark(self, dataframe, columns_name):
        assembler = VectorAssembler(inputCols=columns_name, outputCol="num_features")
        df_assembler = assembler.transform(dataframe)
        scaler = PySparkMinMaxScaler(inputCol="num_features", outputCol="scaled_features")
        scaler_model = scaler.fit(df_assembler)
        df_scaled = scaler_model.transform(df_assembler)
        return df_scaled
