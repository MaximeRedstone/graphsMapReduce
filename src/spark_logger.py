import pandas as pd
import warnings
import os
import logging
import logzero
from logzero import logger
from datetime import datetime
import numpy as np

warnings.filterwarnings('ignore')

MILL_SECS_TO_SECS = 1000

class SparkLogger():

    def __init__(self, filepath):

        self.filepath = filepath
        self.log_df = pd.read_json(self.filepath, lines=True)


    def filter_df(self, event_types, log_df):
    
        df_list = []
        for event_type in event_types: 
            df = log_df[log_df['Event'] == event_type]
            df.dropna(axis=1, how='all', inplace=True)
            df_list.append(df)
        
        return df_list


    def calculate_time_duration(self, df, column_start='Submission Time', column_end='Completion Time'):
        df['Duration'] = (df[column_end] - df[column_start]) / MILL_SECS_TO_SECS
        df[column_end] = df[column_end].apply(lambda x: datetime.utcfromtimestamp(x/MILL_SECS_TO_SECS).strftime('%Y-%m-%d %H:%M:%S.%f'))
        df[column_start] = df[column_start].apply(lambda x: datetime.utcfromtimestamp(x/MILL_SECS_TO_SECS).strftime('%Y-%m-%d %H:%M:%S.%f'))
        df[column_end] = pd.to_datetime(df[column_end], format='%Y-%m-%d %H:%M:%S.%f')
        df[column_start] = pd.to_datetime(df[column_start], format='%Y-%m-%d %H:%M:%S.%f')
        return df

    def generate_jobs(self):

        event_types = ['SparkListenerJobStart', 'SparkListenerJobEnd']
        job_list_df = self.filter_df(event_types, self.log_df)
        job_df = job_list_df[0].merge(job_list_df[1], on=['Job ID'])
        job_df = self.calculate_time_duration(job_df)
        job_df['Job ID'] = job_df['Job ID'].astype(int)
        job_df.drop(columns=['Event_x', 'Event_y'], inplace=True)

        properties_list = []
        for index, row in job_df.iterrows():
            tmp_df = row['Properties']
            tmp_df = pd.DataFrame.from_records([tmp_df])
            properties_list.append(tmp_df)
        properties = pd.concat(properties_list)

        job_df.reset_index(inplace=True)
        properties.reset_index(inplace=True)

        job_df = job_df.merge(properties, left_index=True, right_index=True)
        job_df.set_index(['Job ID'], inplace=True)
        job_df.drop(['index_x',	'Properties', 'index_y'], axis=1, inplace=True)
        job_df['Filename'] = "Not found"

        self.job_df = job_df


    def unpack_stages_from_jobs(self):
        """ Unpack nested stages info from jobs df and adds job foreign key
        """

        stage_df_list = []
        for index, row in self.job_df.iterrows():
            tmp_df = row['Stage Infos']
            tmp_df = pd.DataFrame(tmp_df)
            tmp_df['Job ID'] = index
            stage_df_list.append(tmp_df)
        
        self.stage_df = pd.concat(stage_df_list)
        self.stage_df.set_index(['Stage ID'], inplace=True)
        self.stage_df = self.stage_df[['Job ID']]
    
    def generate_stages(self):

        event_types = ['SparkListenerStageCompleted']
        stage_df = self.filter_df(event_types, self.log_df)[0]

        info_df_list, rdd_info_list = [], []
        for index, row in stage_df.iterrows():
            
            tmp_df = row['Stage Info']       
            rdd_info = tmp_df['RDD Info']
            rdd_info_df = pd.DataFrame(rdd_info)
            
            rdd_info_df['Stage ID'] = tmp_df['Stage ID']
            rdd_info_list.append(rdd_info_df)
            
            tmp_df = pd.DataFrame.from_dict(tmp_df, orient='index')
            tmp_df = tmp_df.transpose()
            tmp_df.set_index(['Stage ID'], inplace=True)
            info_df_list.append(tmp_df)
            
        info_df = pd.concat(info_df_list)
        self.rdd_info_df = pd.concat(rdd_info_list)
        
        stage_df.reset_index(inplace=True)
        info_df.reset_index(inplace=True)

        stage_df = stage_df.merge(info_df, left_index=True, right_index=True)
        stage_df.set_index(['Stage ID'], inplace=True)
        self.stage_df = self.stage_df.merge(stage_df, left_index=True, right_index=True)
        self.stage_df.drop(['index', 'Event', 'Stage Info', 'RDD Info', 'Accumulables'], axis=1, inplace=True)
        self.stage_df = self.calculate_time_duration(self.stage_df)
        self.stage_df.sort_index(inplace=True)

        self.rdd_info_df.set_index('RDD ID', inplace=True)
        self.rdd_info_df.sort_index(inplace=True)
        

    def generate_tasks(self):

        task_types = ['SparkListenerTaskEnd']
        task_df = self.filter_df(task_types, self.log_df)[0]
                    
        info_df_list = []
        accum_list = []
        for index, row in task_df.iterrows():
            
            tmp_df = row['Task Info']
            tmp_df = pd.DataFrame.from_dict(tmp_df, orient='index')
            tmp_df = tmp_df.transpose()
            tmp_df['Stage ID'] = int(row['Stage ID'])
            tmp_df.set_index(['Task ID'], inplace=True)
            info_df_list.append(tmp_df)

            accum_df = tmp_df['Accumulables'].values
            accum_df = pd.DataFrame(accum_df[0])
            accum_df['Task ID'] = tmp_df.index.values[0]
            accum_list.append(accum_df)
        
        self.tasks_df = pd.concat(info_df_list)
        self.tasks_df = self.calculate_time_duration(self.tasks_df, 'Launch Time', 'Finish Time')
        self.tasks_df.drop(['Index', 'Accumulables'], axis=1, inplace=True)

        self.accumulables_df = pd.concat(accum_list)
        self.accumulables_df.rename({'ID': 'Accumulable ID'}, axis=1, inplace=True)
        self.accumulables_df.set_index(['Accumulable ID', 'Task ID'], inplace=True)
        self.accumulables_df.sort_index(inplace=True)


    def write_files(self, root_path, comment=""):

        self.job_df.drop(columns=['Stage Infos'], axis=1, inplace=True)
        
        print(f"Jobs head filename = {self.job_df.head()} \n Column Filename Unique = {self.job_df['Filename'].unique().tolist()}")
        list_filenames = self.job_df['Filename'].unique().tolist()
        list_filenames = [x.split('/')[-1] for x in list_filenames]
        logger.info(f"Writing logs for files {list_filenames}")

        if not os.path.exists(root_path):
            os.mkdir(root_path)
        
        for filename in list_filenames:
            print(f"Creating root_path_run")
            root_path_run = os.path.join(root_path, filename + comment)
            if not os.path.exists(root_path_run):
                os.mkdir(root_path_run)
            
            print(f"select jobs for filename {filename}")
            job_df = self.job_df[self.job_df['Filename'] == filename]
            list_jobs = np.unique(job_df.index.values).tolist()

            stage_df = self.stage_df[self.stage_df['Job ID'].isin(list_jobs)]
            list_stages = np.unique(stage_df.index.values).tolist()

            tasks_df = self.tasks_df[self.tasks_df['Stage ID'].isin(list_stages)]
            rdd_info_df = self.rdd_info_df[self.rdd_info_df['Stage ID'].isin(list_stages)]
       
            list_tasks = np.unique(tasks_df.index.values).tolist()
            accumulables_df = self.accumulables_df[self.accumulables_df.index.get_level_values('Task ID').isin(list_tasks)]

            print(f"Writing csv files for {root_path_run}")
            job_df.to_csv(os.path.join(root_path_run, "log_jobs.csv"), sep=";")
            stage_df.to_csv(os.path.join(root_path_run, "log_stages.csv"), sep=";")
            tasks_df.to_csv(os.path.join(root_path_run, "log_tasks.csv"), sep=";")
            rdd_info_df.to_csv(os.path.join(root_path_run, "log_rdds.csv"), sep=";")
            accumulables_df.to_csv(os.path.join(root_path_run, "log_accumulables.csv"), sep=";")
            print(f"Finishing writing csv files for {root_path_run}")


    def get_filename(self):

        print(f"self.rdd_info_df['Name'].str.contains('dbfs') = {self.rdd_info_df['Name'].str.contains('dbfs')}")
        print(f"self.rdd_info_df = {self.rdd_info_df.head()}")
        filenames = self.rdd_info_df[self.rdd_info_df['Name'].str.contains("dbfs")]
        print(f"all filenames are = {filenames}")
        filenames.drop_duplicates(subset=['Name'], keep='first', inplace=True)
        filenames = filenames[['Name', 'Stage ID']]
        filenames.set_index(['Name'], inplace=True)
        filenames.rename({'Stage ID': 'min_stage_id'}, inplace=True, axis=1)
        filenames['max_stage_id'] = 0
        for idx in range(len(filenames)):
            try:
                filenames.iloc[idx, len(filenames.columns) - 1] = filenames.iloc[idx + 1, len(filenames.columns) - 2] - 1
            except:
                filenames.iloc[idx, len(filenames.columns) - 1] = self.stage_df.index.values[len(self.stage_df) - 1]

        stage_jobs = self.stage_df.drop_duplicates(subset=['Job ID'], keep='first', inplace=False)
        for index_stage, row_stage in stage_jobs.iterrows():
            for index_filename, row_filename in filenames.iterrows():
                if index_stage >= row_filename['min_stage_id'] and index_stage <= row_filename['max_stage_id']:
                    print(f"index filename = {index_filename}")
                    self.job_df.loc[row_stage['Job ID'], 'Filename'] = index_filename.split('/')[-1]
                    break



    def generate_database(self):

        logger.info("Starting generate_jobs")
        self.generate_jobs()

        logger.info("Starting unpack_stages_from_jobs")
        self.unpack_stages_from_jobs()

        logger.info("Starting generate_stages")
        self.generate_stages()

        logger.info("Starting generate_tasks")
        self.generate_tasks()

        self.get_filename()
