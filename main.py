# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#Service Account: mssql-restore-test@ti-is-devenv-01.iam.gserviceaccount.com
#SET GOOGLE_APPLICATION_CREDENTIALS=C:\PHome\GCPSQLAutoIn\ti-dba-automate.json

# Import the necessary packages
#from consolemenu import *
#from consolemenu.items import *
import argparse
from googleapiclient.discovery import build
from google.auth import compute_engine
import pandas as pd
import modules
from modules import *
import storage
from storage import *

import datetime
import logging
import os
import sys
import io

logger = logging.getLogger()
project = ""
projects = []
instances = []

def CloudSQLCollector(arg1,arg2):
    computer = build('cloudresourcemanager', 'v1')
    projects = list_projects(computer)

    instances = []
    for project in projects:
        instances = instances + cloudsqlinstances(project['NAME'])
        instance_df = pd.DataFrame(instances)
        # text buffer
        s_buf = io.StringIO()
        # saving a data frame to a buffer (same as with a regular file):
        instance_df.to_csv(s_buf)
        s_buf.seek(0)
        bucket(s_buf,'instances.csv')

#    for project in projects:
    databases = cloudsqldatabases(instances)
    instance_df = pd.DataFrame(instances)
    databases_df = pd.DataFrame(databases)
    databases_final_df = pd.merge(instance_df, databases_df, on=['project','instance'])
    # text buffer
    s_buf = io.StringIO()
    # saving a data frame to a buffer (same as with a regular file):
    databases_final_df.to_csv(s_buf)
    s_buf.seek(0)
    bucket(s_buf,'databases.csv')

#    for project in projects:
    users = cloudsqlusers(instances)
    instance_df = pd.DataFrame(instances)
    users_df = pd.DataFrame(users)
    users_final_df = pd.merge(instance_df, users_df, on=['project','instance'])
    # text buffer
    s_buf = io.StringIO()
    # saving a data frame to a buffer (same as with a regular file):
    users_final_df.to_csv(s_buf)
    s_buf.seek(0)
    bucket(s_buf,'users.csv')

    databases = cloudsqldatabases2(instances)
    instance_df = pd.DataFrame(instances)
    databases_df = pd.DataFrame(databases)
    databases_final_df = pd.merge(instance_df, databases_df, on=['project','instance'])
    # text buffer
    s_buf = io.StringIO()
    # saving a data frame to a buffer (same as with a regular file):
    databases_final_df.to_csv(s_buf)
    s_buf.seek(0)
    bucket(s_buf,'db_inside.csv')


    #for project in projects:
    #    instances = cloudsqlinstances(project['NAME'])
    #    users = cloudsqlusers(project['NAME'],instances)
    #    grants = cloudsqlgrants(project['NAME'],instances)
    #    instance_df = pd.DataFrame(instances)
    #    users_df = pd.DataFrame(users)
    #    grants_df = pd.DataFrame(grants)

    #    users_final_df = pd.merge(instance_df, users_df, on=['project','instance'])
    #    grants_final_df = pd.merge(users_final_df, grants_df, on=['project','instance','user'])

        # text buffer
    #    s_buf = io.StringIO()
        # saving a data frame to a buffer (same as with a regular file):
    #    users_final_df.to_csv(s_buf)
    #    s_buf.seek(0)
    #    bucket(s_buf,'access.csv')


def cloudsqlinstances(proj):
    # Construct the service object for the interacting with the Cloud SQL Admin API.
    #print(proj)
    cloudsql = build('sqladmin','v1beta4')
    if not proj:
        computer = build('cloudresourcemanager', 'v1')
        projects = list_projects(computer)
        for project in projects:
            instances = list_sql_instances(cloudsql, project["NAME"])
    else:
        instances = list_sql_instances(cloudsql, proj)

    return instances

def cloudsqldatabases(instances):
    # Construct the service object for the interacting with the Cloud SQL Admin API.
    cloudsql = build('sqladmin','v1beta4')

    databases = []
    if not instances:
        computer = build('cloudresourcemanager', 'v1')
        projects = list_projects(computer)
        for project in projects:
            instances = list_sql_instances(cloudsql, project["name"])

    if instances:
        for instance in instances:
            databases = databases + list_sql_instance_databases(cloudsql,instance['project'],instance['instance'])
    else:
        databases=["Empty"]

    return databases

def cloudsqlusers(instances):

    # Construct the service object for the interacting with the Cloud SQL Admin API.
    cloudsql = build('sqladmin','v1beta4')

    users = []
    for instance in instances:
        users = users + list_sql_instance_users(cloudsql,instance['project'],instance['instance'])

    return users

def cloudsqldatabases2(instances):

    # Construct the service object for the interacting with the Cloud SQL Admin API.
    cloudsql = build('sqladmin','v1beta4')

    databases = []

    if instances:
        for instance in instances:
            if skipInstance(instance) == 0:
                databases = databases + list_sql_databases(cloudsql,instance)
    else:
        databases=["Empty"]

    return databases

def my_quit_fn():
   raise SystemExit

def invalid():
   print ("INVALID CHOICE!")
