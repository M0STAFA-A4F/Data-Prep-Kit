import pandas as pd


class DataPrepKit:
    
    def read_file(self, file_name):
        fname_split = file_name.split('.')
        df = None
        
        if fname_split[-1] == 'csv':
            df = pd.read_csv(file_name)
        
        elif fname_split[-1] == 'xlsx':
            df = pd.read_excel(file_name)
        
        elif fname_split[-1] == 'json':
            df = pd.read_json(file_name)
            df = df.replace('', None)
        
        return df
    
    
    def __get_data_type(self, df_column):
        dtype = ''

        if str(type(df_column.describe().dtype)) == "<class 'numpy.dtype[object_]'>":
            dtype = 'string'
        elif str(type(df_column.sum())) == "<class 'numpy.int64'>":
            dtype = 'int'
        elif str(type(df_column.sum())) == "<class 'numpy.float64'>":
            dtype = 'float'

        return dtype
    
    
    def __most_frequent(self, df_column):
        unique = {}
        freq, top = 0, ''

        for item in reversed(df_column):
            unique[item] = unique.get(item, 0) + 1
            if unique[item] >= freq :
                freq, top = unique[item], item

        return unique, top, freq
    
    
    def data_summary(self, df):
        df_null = df.isnull().sum()
        total_data = len(df)
        
        all_df = pd.DataFrame({'':['count', 'missing val', 'missing %', 'unique', 'most freq', 'most freq cnt', 'mean', 'data type']}).set_index('')

        for col in df.columns:
            data = []
            df_describe = df[col].describe()
            unique, top, freq = self.__most_frequent(df[col])

            data.append(df_describe['count'])
            data.append(df_null[col])
            data.append('{:0.2f}%'.format((df_null[col]/total_data)*100))
            data.append(len(unique))
            data.append(top)
            data.append(freq)
            data.append('NaN' if self.__get_data_type(df[col]) == 'string' else df_describe['mean'])
            data.append(self.__get_data_type(df[col]))

            all_df.insert(loc=len(all_df.columns), column=col, value=data)
            
        pd.set_option('display.max_columns', None)
        
        return all_df
    
    
    def row_missing_data_summary(self, df):
        row_missing_count = df.isnull().sum(axis=1)
        total_col = len(df.axes[1])
        category = {
            '0%'          : [0],
            '1%  -> 25%'  : [0],
            '25% -> 50%'  : [0],
            '50% -> 75%'  : [0],
            '75% -> 100%' : [0]
        }
        
        for row in row_missing_count:
            percentage = (row/total_col) * 100
            
            if percentage == 0:
                category['0%'][0] += 1
            elif percentage >= 1 and percentage < 25:
                category['1%  -> 25%'][0] += 1
            elif percentage >= 25 and percentage < 50:
                category['25% -> 50%'][0] += 1
            elif percentage >= 50 and percentage < 75:
                category['50% -> 75%'][0] += 1
            elif percentage >= 75 and percentage <= 100:
                category['75% -> 100%'][0] += 1
        
        return pd.DataFrame(category)
    
    
    def drop_columns(self, df, columns_list):
        return df.drop(columns_list, axis = 1)
    
    
    def drop_col_contains_missing_percentage(self, df, percentage):
        keep_values = (1 - percentage/100) * len(df)
        return df.dropna(thresh=keep_values, axis=1)
    
    
    def drop_row_contains_missing_percentage(self, df, percentage):
        keep_values = (1 - percentage/100) * len(df.axes[1])
        return df.dropna(thresh=keep_values)
    
    
    def replace_string(self, df_column):
        unique_list = df_column.unique()
        df_column_copy = df_column.copy()
        
        cnt = 0
        for unique in unique_list:
            if not pd.isnull(unique):
                df_column_copy = df_column_copy.replace(to_replace = unique, value = cnt)
                cnt += 1
            
        return df_column_copy
    
    
    def __get_all_col_strings(self, df):
        col_list = []
        
        for col in df.columns:
            if self.__get_data_type(df[col]) == 'string':
                col_list.append(col)
                
        return col_list
    
    
    def replace_all_strings(self, df):
        col_list = self.__get_all_col_strings(df)
        df_copy = df.copy()
        
        for col in col_list:
            df_copy[col] = self.replace_string(df_copy[col])

        return df_copy
    
    
    def mean_imputation(self, df_column):
        df_column_copy = pd.Series(df_column)
        mean_value = df_column_copy.mean()
        
        return df_column_copy.fillna(mean_value)
    
    
    def median_imputation(self, df_column):
        df_column_copy = pd.Series(df_column)
        median_value = df_column_copy.median()
        
        return df_column_copy.fillna(median_value)
    
    
    def mode_imputation(self, df_column):
        df_column_copy = pd.Series(df_column)
        _, mode_value, _ = self.__most_frequent(df_column_copy)
        
        return df_column_copy.fillna(mode_value)
    
    
    def forward_fill_imputation(self, df_column):
        df_column_copy = df_column.copy()
        
        df_column_copy = df_column_copy.ffill()

        return df_column_copy
    
    
    def backward_fill_imputation(self, df_column):
        df_column_copy = df_column.copy()
        
        df_column_copy = df_column_copy.bfill()

        return df_column_copy
    
