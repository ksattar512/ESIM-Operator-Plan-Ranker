"""
Developer Signature: Kashif Sattar — ESIM Operator Plan Ranker
Purpose: Data persistence and database query layer.
Public Repo Note: Credentials, database names, schema names, and SSH key paths must come from environment variables.
"""

import math
from Config.global_config import Config
import pymysql
import paramiko
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from datetime import datetime
class Connector:

    """Database connector for ESIM package ranking operations."""
    def __init__(self):

        """Initialize database, SSH, and runtime configuration values for this component."""
        self.rds_host  = Config.HOST_NAME
        self.name = Config.USER_NAME
        self.password = Config.PASSWORD
        self.db_name = Config.DB
        self.sql_hostname = Config.HOST_NAME
        self.sql_username = Config.USER_NAME
        self.sql_password = Config.PASSWORD
        self.Hsql_password = Config.HPASSWORD
        self.sql_main_database = Config.DB
        self.sql_port = 3306
        self.ssh_host = Config.ssh_host
        self.ssh_user = Config.ssh_user
        self.ssh_port = Config.ssh_port
        self.sql_ip = Config.sql_ip
        pkeyfilepath = Config.SSH_PRIVATE_KEY_PATH
        self.mypkey = paramiko.RSAKey.from_private_key_file(pkeyfilepath) if pkeyfilepath else None

    def public_function(self):
        """Simple public connectivity/test function for this component."""
        print("Public function invoked")

    def fetch_country_via_ssh(self):
        """Fetch the list of country codes used as ranking input."""
        with SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_pkey=self.mypkey,
            remote_bind_address=(self.sql_hostname, self.sql_port)) as tunnel:
            conn = pymysql.connect(host='127.0.0.1', user=self.sql_username,
                passwd=self.sql_password, db=self.sql_main_database,
                port=tunnel.local_bind_port)
            query = """SELECT countryCode FROM [SchemaName].localcountries where countryCode="SA";"""
            records = pd.read_sql_query(query, conn)
            records_list = records['countryCode'].tolist()
            conn.close()
            print(records_list)
            return records_list

    def fetch_data_via_ssh(self):

        """Fetch the available data bundle values used as ranking input."""
        with SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_pkey=self.mypkey,
            remote_bind_address=(self.sql_hostname, self.sql_port)) as tunnel:
            conn = pymysql.connect(host='127.0.0.1', user=self.sql_username,
                passwd=self.sql_password, db=self.sql_main_database,
                port=tunnel.local_bind_port)
            query = """SELECT distinct data_text FROM [SchemaName].lcr_tblcarrier_package;"""
            records = pd.read_sql_query(query, conn)
            records_list = records['data_text'].tolist()
            print("Connection is successful")
            conn.close()
            print(records_list)
            return records_list

    def fetch_days_via_ssh(self):

        """Fetch available package duration values used as ranking input."""
        with SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_pkey=self.mypkey,
            remote_bind_address=(self.sql_hostname, self.sql_port)) as tunnel:
            conn = pymysql.connect(host='127.0.0.1', user=self.sql_username,
                passwd=self.sql_password, db=self.sql_main_database,
                port=tunnel.local_bind_port)
            query = """SELECT distinct days FROM [SchemaName].lcr_tblcarrier_package;"""
            records = pd.read_sql_query(query, conn)
            record_list = records['days'].tolist()
            conn.close()
            print(record_list)
            return record_list

    def fetch_Packages_via_ssh(self):
        """Fetch package names through the configured SSH/database connection."""
        with SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_pkey=self.mypkey,
            remote_bind_address=(self.sql_hostname, self.sql_port)) as tunnel:
            conn = pymysql.connect(host='127.0.0.1', user=self.sql_username,
                passwd=self.sql_password, db=self.sql_main_database,
                port=tunnel.local_bind_port)
            query = """SELECT * FROM [SchemaName].lcr_tblcarrier_package;"""
            records = pd.read_sql_query(query, conn)
            print("Connection is successful")
            print(records)

        conn.close()



    def fetch_records_array_ssh(self, conn ,Country, Data, Duration):

                """Fetch package records matching country, data, duration, or ranking filters."""
                query = """SELECT * FROM [SchemaName].lcr_tblcarrier_package where countries='%s' AND data_text='%s' AND days=%s;"""
                query = query % (Country, Data, Duration)

                records = pd.read_sql_query(query, conn)
                print(records)
                records_list = records.to_dict('records')
                print(records_list)

                return records_list

    def fetch_records_array_ssh_WDays(self, conn ,Country, Data):

                """Fetch package records matching country, data, duration, or ranking filters."""
                query = """SELECT * FROM [SchemaName].lcr_tblcarrier_package where countries='%s' AND data_text='%s';"""
                query = query % (Country, Data)

                records = pd.read_sql_query(query, conn)
                print(records)
                records_list = records.to_dict('records')
                print(records_list)

                return records_list

    def fetch_records_array_ssh_WDays2(self, conn ,Country, Data,Days):

                """Fetch package records matching country, data, duration, or ranking filters."""
                query = """SELECT * FROM [SchemaName].lcr_tblcarrier_package where countries='%s' AND data_text='%s' AND days=%s limit 3;"""
                query = query % (Country, Data, Days)

                records = pd.read_sql_query(query, conn)
                print(records)
                records_list = records.to_dict('records')
                print(records_list)

                return records_list

    def fetch_records_array_StrPackage_ssh(self, conn ,Country, Data, Duration):

                """Fetch package records matching country, data, duration, or ranking filters."""
                query = """SELECT * FROM [SchemaName].srtpackage where countries in ('BQ-SA');"""

                records = pd.read_sql_query(query, conn)
                print(records)
                records_list = records.to_dict('records')
                print(records_list)

                return records_list

    def fetch_records_array_StrPackage_V1(self, conn ,Country, Data, Duration):

                """Fetch package records matching country, data, duration, or ranking filters."""
                conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

                query = """SELECT * FROM [SchemaName].srtpackage;"""

                records = pd.read_sql_query(query, conn)
                print(records)
                records_list = records.to_dict('records')
                print(records_list)

                return records_list

    def validate_record_ssh(self,conn,packagename):
                """Check whether a package/ranked record already exists before insert or update."""
                query = """SELECT * FROM [SchemaName].srtpackage where package='%s';"""
                query = query % (packagename)
                records = pd.read_sql_query(query, conn)
                records_list = records.to_dict('records')
                print(records)
                print(records_list)

                if records_list:
                    return True
                else:
                    return False

    def validate_record_ssh_WDays(self,conn,packagename):
                """Check whether a package/ranked record already exists before insert or update."""
                query = """SELECT * FROM [SchemaName].srtpackage2 where package='%s';"""
                query = query % (packagename)
                records = pd.read_sql_query(query, conn)
                records_list = records.to_dict('records')
                print(records)
                print(records_list)

                if records_list:
                    return True
                else:
                    return False

    def validate_record_ssh_(self,conn,packagename):
                """Check whether a package/ranked record already exists before insert or update."""
                query = """SELECT * FROM [SchemaName].srtpackage where package='%s';"""
                query = query % (packagename)
                records = pd.read_sql_query(query, conn)
                records_list = records.to_dict('records')
                print(records)
                print(records_list)

                if records_list:
                    return True
                else:
                    return False

    def update_srtpackage_via_ssh(self, conn, package, country, data, days, price, carrier_id, created_date, updated_date, activity_state, ranking, country_id, package_type, package_title, status, suggested_saleprice, autoupdate, manual_saleprice, manual_validity_date_start, manual_validity_date_end, suggested_saleprice_criteria, suggested_saleprice_competator):

                    """Update an existing sorted package record with newly calculated ranking values."""
                    if country_id is None:
                        country_id = 0
                    if package is None:
                        package = ""
                    if country is None:
                        country = ""
                    if data is None:
                        data = ""
                    if days is None:
                        days = 0
                    if price is None:
                        price = 0.0
                    if carrier_id is None:
                        carrier_id = 0
                    if created_date is None:
                        created_date = '0000-00-00 00:00:00'
                    if updated_date is None:
                        updated_date = '0000-00-00 00:00:00'
                    if activity_state is None:
                        activity_state = 0
                    if ranking is None:
                        ranking = 0
                    if package_type is None:
                        package_type = ""
                    if package_title is None:
                        package_title = ""
                    if status is None:
                        status = ""
                    if suggested_saleprice is None:
                        suggested_saleprice = 0.0
                    if autoupdate is None:
                        autoupdate = 0
                    if manual_saleprice is None:
                        manual_saleprice = 0.0
                    if manual_validity_date_start is None:
                        manual_validity_date_start = '0000-00-00 00:00:00'
                    if manual_validity_date_end is None:
                        manual_validity_date_end = '0000-00-00 00:00:00'
                    if suggested_saleprice_criteria is None:
                        suggested_saleprice_criteria = ""
                    if suggested_saleprice_competator is None:
                        suggested_saleprice_competator = ""

                    with conn.cursor() as cur:
                        sql = """
                            UPDATE `[SchemaName]`.`srtpackage`
                            SET
                                `package` = %s,
                                `country` = %s,
                                `data` = %s,
                                `days` = %s,
                                `price` = %s,
                                `carrier_id` = %s,
                                `created_date` = %s,
                                `updated_date` = %s,
                                `activity_state` = %s,
                                `ranking` = %s,
                                `country_id` = %s,
                                `package_type` = %s,
                                `package_title` = %s,
                                `status` = %s,
                                `suggested_saleprice` = %s,
                                `autoupdate` = %s,
                                `manual_saleprice` = %s,
                                `manual_validity_date_start` = %s,
                                `manual_validity_date_end` = %s,
                                `suggested_saleprice_criteria` = %s,
                                `suggested_saleprice_competator` = %s
                            WHERE `package` = %s;  -- Assuming 'package' is a unique identifier
                        """
                        val = (package, country, data, days, price, carrier_id, created_date, updated_date, activity_state, ranking, country_id, package_type, package_title, status, suggested_saleprice, autoupdate, manual_saleprice, manual_validity_date_start, manual_validity_date_end, suggested_saleprice_criteria, suggested_saleprice_competator, package)  # Added 'package' again for the WHERE clause
                        cur.execute(sql, val)
                        conn.commit()
                    return cur.rowcount  # Returns the number of rows updated

    def insert_srtpackage_via_ssh(self, conn, package,country,data,days,price,carrier_id,created_date,updated_date,activity_state,ranking,country_id,package_type,package_title,status,suggested_saleprice,autoupdate,manual_saleprice,manual_validity_date_start,manual_validity_date_end,suggested_saleprice_criteria,suggested_saleprice_competator):

        """Insert a ranked package record into the sorted package table."""
        if country_id == None:
            country_id = 0
        if package is None:
            package = ""
        if country is None:
            country = ""
        if data is None:
            data = ""
        if days is None:
            days = 0
        if price is None:
            price = 0.0
        if carrier_id is None:
            carrier_id = 0
        if created_date is None:
            created_date = '0000-00-00 00:00:00'
        if updated_date is None:
            updated_date = '0000-00-00 00:00:00'
        if activity_state is None:
            activity_state = 0
        if ranking is None:
            ranking = 0
        if package_type is None:
            package_type = ""
        if package_title is None:
            package_title = ""
        if status is None:
            status = ""
        if suggested_saleprice is None:
            suggested_saleprice = 0.0
        if autoupdate is None:
            autoupdate = 0
        if manual_saleprice is None:
            manual_saleprice = 0.0
        if manual_validity_date_start is None:
            manual_validity_date_start = '0000-00-00 00:00:00'
        if manual_validity_date_end is None:
            manual_validity_date_end = '0000-00-00 00:00:00'
        if suggested_saleprice_criteria is None:
            suggested_saleprice_criteria = ""
        if suggested_saleprice_competator is None:
            suggested_saleprice_competator = ""

        with conn.cursor() as cur:
                    sql="""INSERT INTO `[SchemaName]`.`srtpackage` (`package`,`country`,`data`,`days`,`price`,`carrier_id`,`created_date`,`updated_date`,`activity_state`,`ranking`,`country_id`,`package_type`,`package_title`,`status`,
`suggested_saleprice`,`autoupdate`,`manual_saleprice`,`manual_validity_date_start`,`manual_validity_date_end`,`suggested_saleprice_criteria`,`suggested_saleprice_competator`)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
                    val = (package, country, data, days, price,carrier_id,created_date,updated_date,activity_state,ranking,country_id,package_type,package_title,status,suggested_saleprice,autoupdate,manual_saleprice,manual_validity_date_start,manual_validity_date_end,suggested_saleprice_criteria,suggested_saleprice_competator)
                    cur.execute(sql, val)
                    conn.commit()
        return cur.lastrowid

    def insert_srtpackage2_via_ssh(self, conn, package,country,data,days,price,carrier_id,created_date,updated_date,activity_state,ranking,country_id,package_type,package_title,status,suggested_saleprice,autoupdate,manual_saleprice,manual_validity_date_start,manual_validity_date_end,suggested_saleprice_criteria,suggested_saleprice_competator):

        """Insert a ranked package record into the sorted package table."""
        if  country_id == None or math.isnan(country_id):
            country_id = 0
        if package is None:
            package = ""
        if country is None:
            country = ""
        if data is None:
            data = ""
        if days == None or math.isnan(days):
            days = 0
        if price is None:
            price = 0.0
        if carrier_id is None:
            carrier_id = 0
        if created_date is None:
            created_date = '0000-00-00 00:00:00'
        if updated_date is None:
            updated_date = '0000-00-00 00:00:00'
        if activity_state is None:
            activity_state = 0
        if ranking is None:
            ranking = 0
        if package_type is None:
            package_type = ""
        if package_title is None:
            package_title = ""
        if status is None:
            status = ""
        if suggested_saleprice is None:
            suggested_saleprice = 0.0
        if autoupdate is None:
            autoupdate = 0
        if manual_saleprice is None:
            manual_saleprice = 0.0
        if manual_validity_date_start is None:
            manual_validity_date_start = '0000-00-00 00:00:00'
        if manual_validity_date_end is None:
            manual_validity_date_end = '0000-00-00 00:00:00'
        if suggested_saleprice_criteria is None:
            suggested_saleprice_criteria = ""
        if suggested_saleprice_competator is None:
            suggested_saleprice_competator = ""

        with conn.cursor() as cur:
                    sql="""INSERT INTO `[SchemaName]`.`srtpackage2` (`package`,`country`,`data`,days,`price`,`carrier_id`,`created_date`,`updated_date`,`activity_state`,`ranking`,`country_id`,`package_type`,`package_title`,`status`,
`suggested_saleprice`,`autoupdate`,`manual_saleprice`,`manual_validity_date_start`,`manual_validity_date_end`,`suggested_saleprice_criteria`,`suggested_saleprice_competator`)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
                    val = (package, country, data,days, price,carrier_id,created_date,updated_date,activity_state,ranking,country_id,package_type,package_title,status,suggested_saleprice,autoupdate,manual_saleprice,manual_validity_date_start,manual_validity_date_end,suggested_saleprice_criteria,suggested_saleprice_competator)
                    cur.execute(sql, val)
                    conn.commit()
        return cur.lastrowid


    def update_srtpackage2_via_ssh(self, conn, package, country, data, days, price, carrier_id, created_date, updated_date, activity_state, ranking, country_id, package_type, package_title, status, suggested_saleprice, autoupdate, manual_saleprice, manual_validity_date_start, manual_validity_date_end, suggested_saleprice_criteria, suggested_saleprice_competator):

                    """Update an existing sorted package record with newly calculated ranking values."""
                    if country_id is None or  math.isnan(country_id):
                       country_id = 0
                    if package is None:
                        package = ""
                    if country is None:
                        country = ""
                    if data is None:
                        data = ""
                    if days is None or math.isnan(days) :
                        days = 0
                    if price is None:
                        price = 0.0
                    if carrier_id is None:
                        carrier_id = 0
                    if created_date is None:
                        created_date = '0000-00-00 00:00:00'
                    if updated_date is None:
                        updated_date = '0000-00-00 00:00:00'
                    if activity_state is None:
                        activity_state = 0
                    if ranking is None:
                        ranking = 0
                    if package_type is None:
                        package_type = ""
                    if package_title is None:
                        package_title = ""
                    if status is None:
                        status = ""
                    if suggested_saleprice is None:
                        suggested_saleprice = 0.0
                    if autoupdate is None:
                        autoupdate = 0
                    if manual_saleprice is None:
                        manual_saleprice = 0.0
                    if manual_validity_date_start is None:
                        manual_validity_date_start = '0000-00-00 00:00:00'
                    if manual_validity_date_end is None:
                        manual_validity_date_end = '0000-00-00 00:00:00'
                    if suggested_saleprice_criteria is None:
                        suggested_saleprice_criteria = ""
                    if suggested_saleprice_competator is None:
                        suggested_saleprice_competator = ""

                    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    with conn.cursor() as cur:
                        sql = """
                            UPDATE `[SchemaName]`.`srtpackage2`
                            SET
                                `package` = %s,
                                `country` = %s,
                                `data` = %s,
                                 `days` = %s,
                                `price` = %s,
                                `carrier_id` = %s,
                                `created_date` = %s,
                                `updated_date` = %s,
                                `activity_state` = %s,
                                `ranking` = %s,
                                `country_id` = %s,
                                `package_type` = %s,
                                `package_title` = %s,
                                `status` = %s,
                                `suggested_saleprice` = %s,
                                `autoupdate` = %s,
                                `manual_saleprice` = %s,
                                `manual_validity_date_start` = %s,
                                `manual_validity_date_end` = %s,
                                `suggested_saleprice_criteria` = %s,
                                `suggested_saleprice_competator` = %s,
                                `LastUpdated` = %s


                            WHERE `package` = %s;  -- Assuming 'package' is a unique identifier
                        """
                        val = (package, country, data,days, price, carrier_id, created_date, updated_date, activity_state, ranking, country_id, package_type, package_title, status, suggested_saleprice, autoupdate, manual_saleprice, manual_validity_date_start, manual_validity_date_end, suggested_saleprice_criteria, suggested_saleprice_competator, current_date,package)  # Added 'package' again for the WHERE clause
                        cur.execute(sql, val)
                        conn.commit()
                    return cur.rowcount  # Returns the number of rows updated


    def fetch_country(self):
            """Fetch the list of country codes used as ranking input."""
            conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.Hsql_password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

            with conn.cursor() as cur:
                sql = """SELECT countryCode FROM [SchemaName].localcountries;"""
                cur.execute(sql)
                conn.commit()
                records = cur.fetchall()
                records_list = [record['countryCode'] for record in records]
                conn.close()
                return records_list

            cur.close()

    def fetch_pkg_data(self):
            """Fetch the available data bundle values used as ranking input."""
            conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

            with conn.cursor() as cur:
                sql = """SELECT distinct data_text FROM [SchemaName].lcr_tblcarrier_package;"""
                cur.execute(sql)
                conn.commit()
                records = cur.fetchall()
                conn.close()
                if not records:
                    return []
                else:
                    return records

            cur.close()

    def fetch_pkg_days(self):
            """Fetch available package duration values used as ranking input."""
            conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

            with conn.cursor() as cur:
                sql = """SELECT distinct days FROM [SchemaName].lcr_tblcarrier_package;"""
                cur.execute(sql)
                conn.commit()
                records = cur.fetchall()
                conn.close()
                if not records:
                    return []
                else:
                    return records

            cur.close()

    def fetch_records_array(self, Country, Data, Duration):

        """Fetch package records matching country, data, duration, or ranking filters."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)
        query = """SELECT * FROM [SchemaName].lcr_tblcarrier_package where countries='%s' AND data_text='%s' AND days='%s';"""
        query = query % (Country, Data,Duration)

        records = pd.read_sql_query(query, conn)
        print(records)
        records_list = records.to_dict('records')
        print(records_list)

        return records_list



    def insert_srtpackage(self, package, country, data, days, price):
        """Insert a ranked package record into the sorted package table."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)

        with conn.cursor() as cur:
                sql = """INSERT INTO [SchemaName].srtpackage (package, country, data, days, price) VALUES (%s, %s, %s, %s, %s);"""
                val = (package, country, data, days, price)
                cur.execute(sql, val)
                conn.commit()
                cur.close()
        return cur.lastrowid

    def validate_record(self, column_name, value):
        """Check whether a package/ranked record already exists before insert or update."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.Hsql_password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)

        with conn.cursor() as cur:
            sql = f"SELECT * FROM [SchemaName].srtpackage WHERE {column_name}=%s;"
            cur.execute(sql, (value,))
            conn.commit()
            record = cur.fetchone()
            if record:
                return True
            else:
                return False

        cur.close()



    def insert_records(self, records):
        """Insert package records into the configured database table."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            sql = """INSERT INTO [SchemaName].carrierpackage (countries, data_text, days, net_price, gross_price, currency, url) VALUES (%s, %s, %s, %s, %s, %s, %s);"""
            val = (records['countries'], records['data_text'], records['days'], records['net_price'], records['gross_price'], records['currency'], records['url'])
            cur.execute(sql, val)
            conn.commit()
            conn.close()
            return cur.lastrowid

        cur.close()

    def update_record(self, records):
        """Update existing package records in the configured database table."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)

        with conn.cursor() as cur:
            sql = """UPDATE [SchemaName].carrierPackage SET net_price=%s, gross_price=%s, currency=%s, url=%s WHERE countries=%s AND data_text=%s AND days=%s;"""
            val = (records['net_price'], records['gross_price'], records['currency'], records['url'], records['countries'], records['data_text'], records['days'])
            cur.execute(sql, val)
            conn.commit()

        cur.close()
    def update_records(self, records):
        """Update existing package records in the configured database table."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)

        for record in records:
            with conn.cursor() as cur:
                sql = """UPDATE [SchemaName].carrierPackage SET `rank`=%s WHERE id=%s;"""
                val = (record['rank'], record['id'])
                cur.execute(sql, val)
                conn.commit()

        cur.close()

    def update_record(self, records):
            """Update existing package records in the configured database table."""
            conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)


            with conn.cursor() as cur:
                    sql = """UPDATE scrape.carrierPackage SET `rank`=%s WHERE id=%s;"""
                    val = (records['rank'], records['id'])
                    cur.execute(sql, val)
                    conn.commit()
            cur.close()



    def fetch_country_V1(self):
            """Fetch the list of country codes used as ranking input."""
            conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.Hsql_password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

            with conn.cursor() as cur:
                sql = """SELECT distinct countryCode FROM [SchemaName].localcountries;"""

                cur.execute(sql)
                conn.commit()
                records = cur.fetchall()
                records_list = [record['countryCode'] for record in records]
                conn.close()
                return records_list

            cur.close()

    def fetch_data_V1(self):

            """Fetch the available data bundle values used as ranking input."""
            conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.Hsql_password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

            with conn.cursor() as cur:

                sql = """SELECT distinct data_text FROM [SchemaName].lcr_tblcarrier_package;"""
                cur.execute(sql)
                conn.commit()
                records = cur.fetchall()
                records_list = [record['data_text'] for record in records]

                conn.close()
                return records_list

            cur.close()

    def fetch_days_V1(self):

            """Fetch available package duration values used as ranking input."""
            conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.Hsql_password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

            with conn.cursor() as cur:
                sql = """SELECT distinct days FROM [SchemaName].lcr_tblcarrier_package;"""
                cur.execute(sql)
                conn.commit()
                records = cur.fetchall()
                records_list = [record['days'] for record in records]
                conn.close()
                return records_list

            cur.close()

    def fetch_records_exc_packages(self, excluded_packages, country, data):
        """Fetch package records matching country, data, duration, or ranking filters."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.Hsql_password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)
        placeholders = ', '.join(['%s'] * len(excluded_packages))
        query = f"""
            SELECT * FROM [SchemaName].srtpackage2
            WHERE country=%s AND data=%s AND package NOT IN ({placeholders});
        """
        params = [country, data] + excluded_packages

        query = query % (params)

        with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
                records = cur.fetchall()
                conn.close()

        cur.close()
        return records

    def fetch_records_array_V1(self, Country, Data, Duration):

        """Fetch package records matching country, data, duration, or ranking filters."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.Hsql_password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)


        with conn.cursor() as cur:
                if Duration is None or Duration == "":

                    query = """SELECT * FROM [SchemaName].lcr_tblcarrier_package where countries='%s' AND data_text='%s' limit 2;"""
                    query = query % (Country, Data)

                else:

                    query = """SELECT * FROM [SchemaName].lcr_tblcarrier_package where countries='%s' AND data_text='%s' AND days='%s' limit 2;"""
                    query = query % (Country, Data, Duration)
                cur.execute(query)
                conn.commit()
                records = cur.fetchall()
                conn.close()
                return records

        cur.close()

    def fetch_records_array_SrtPackage_V1(self, Country, Data, Duration):

        """Fetch package records matching country, data, duration, or ranking filters."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.Hsql_password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)


        with conn.cursor() as cur:
                query = """SELECT * FROM [SchemaName].srtpackage where Country in ('BQ-SA');"""

                cur.execute(query)
                conn.commit()
                records = cur.fetchall()
                conn.close()
                return records

        cur.close()
    def validate_record_V1(self,value):
        """Check whether a package/ranked record already exists before insert or update."""
        conn = pymysql.connect(host=self.rds_host,
                            user=self.name,
                            password=self.Hsql_password,
                            database=self.db_name,
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)

        with conn.cursor() as cur:
            sql = f"SELECT * FROM [SchemaName].srtpackage2 WHERE package=%s;"
            cur.execute(sql, (value,))
            conn.commit()
            record = cur.fetchone()
            if record:
                return True
            else:
                return False

        cur.close()

    def update_srtpackage_v1(self, package, country, data, days, price, carrier_id, created_date, updated_date, activity_state, ranking, country_id, package_type, package_title, status, suggested_saleprice, autoupdate, manual_saleprice, manual_validity_date_start, manual_validity_date_end, suggested_saleprice_criteria, suggested_saleprice_competator):

                    """Update an existing sorted package record with newly calculated ranking values."""
                    if country_id is None:
                        country_id = 0
                    if package is None:
                        package = ""
                    if country is None:
                        country = ""
                    if data is None:
                        data = ""
                    if days is None:
                        days = 0
                    if price is None:
                        price = 0.0
                    if carrier_id is None:
                        carrier_id = 0
                    if created_date is None:
                        created_date = '0000-00-00 00:00:00'
                    if updated_date is None:
                        updated_date = '0000-00-00 00:00:00'
                    if activity_state is None:
                        activity_state = 0
                    if ranking is None:
                        ranking = 0
                    if package_type is None:
                        package_type = ""
                    if package_title is None:
                        package_title = ""
                    if status is None:
                        status = ""
                    if suggested_saleprice is None:
                        suggested_saleprice = 0.0
                    if autoupdate is None:
                        autoupdate = 0
                    if manual_saleprice is None:
                        manual_saleprice = 0.0
                    if manual_validity_date_start is None:
                        manual_validity_date_start = '2025-01-01 00:00:00'
                    if manual_validity_date_end is None:
                        manual_validity_date_end = '2025-01-01 00:00:00'
                    if suggested_saleprice_criteria is None:
                        suggested_saleprice_criteria = ""
                    if suggested_saleprice_competator is None:
                        suggested_saleprice_competator = ""

                    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.Hsql_password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

                    with conn.cursor() as cur:
                        sql = """
                            UPDATE `[SchemaName]`.`srtpackage2`
                            SET
                                `package` = %s,
                                `country` = %s,
                                `data` = %s,
                                `days` = %s,
                                `price` = %s,
                                `carrier_id` = %s,
                                `created_date` = %s,
                                `updated_date` = %s,
                                `activity_state` = %s,
                                `ranking` = %s,
                                `country_id` = %s,
                                `package_type` = %s,
                                `package_title` = %s,
                                `status` = %s,
                                `suggested_saleprice` = %s,
                                `autoupdate` = %s,
                                `manual_saleprice` = %s,
                                `manual_validity_date_start` = %s,
                                `manual_validity_date_end` = %s,
                                `suggested_saleprice_criteria` = %s,
                                `suggested_saleprice_competator` = %s,
                                `LastUpdated` = %s

                            WHERE `package` = %s;  -- Assuming 'package' is a unique identifier
                        """
                        val = (package, country, data, days, price, carrier_id, created_date, updated_date, activity_state, ranking, country_id, package_type, package_title, status, suggested_saleprice, autoupdate, manual_saleprice, manual_validity_date_start, manual_validity_date_end, suggested_saleprice_criteria, suggested_saleprice_competator,current_date, package)  # Added 'package' again for the WHERE clause
                        cur.execute(sql, val)
                        conn.commit()
                        conn.close()
                    return cur.rowcount  # Returns the number of rows updated

    def update_srtpackage_status_v1(self, package,status):

                    """Update the status value for a sorted package record."""
                    if status is None:
                        status = ""

                    conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.Hsql_password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

                    with conn.cursor() as cur:
                        sql = """
                            UPDATE `[SchemaName]`.`srtpackage`
                            SET `status` = %s,
                            WHERE `package` = %s;  -- Assuming 'package' is a unique identifier
                        """
                        val = (package, status)  # Added 'package' again for the WHERE clause
                        cur.execute(sql, val)
                        conn.commit()
                        conn.close()
                    return cur.rowcount  # Returns the number of rows updated

    def insert_srtpackage_v1(self,package,country,data,days,price,carrier_id,created_date,updated_date,activity_state,ranking,country_id,package_type,package_title,status,suggested_saleprice,autoupdate,manual_saleprice,manual_validity_date_start,manual_validity_date_end,suggested_saleprice_criteria,suggested_saleprice_competator):

        """Insert a ranked package record into the sorted package table."""
        if country_id == None:
            country_id = 0
        if package is None:
            package = ""
        if country is None:
            country = ""
        if data is None:
            data = ""
        if days is None:
            days = 0
        if price is None:
            price = 0.0
        if carrier_id is None:
            carrier_id = 0
        if created_date is None:
            created_date = '0000-00-00 00:00:00'
        if updated_date is None:
            updated_date = '0000-00-00 00:00:00'
        if activity_state is None:
            activity_state = 0
        if ranking is None:
            ranking = 0
        if package_type is None:
            package_type = ""
        if package_title is None:
            package_title = ""
        if status is None:
            status = ""
        if suggested_saleprice is None:
            suggested_saleprice = 0.0
        if autoupdate is None:
            autoupdate = 0
        if manual_saleprice is None:
            manual_saleprice = 0.0
        if manual_validity_date_start is None:
            manual_validity_date_start = '2025-01-01 00:00:00'
        if manual_validity_date_end is None:
            manual_validity_date_end = '2025-01-01 00:00:00'
        if suggested_saleprice_criteria is None:
            suggested_saleprice_criteria = ""
        if suggested_saleprice_competator is None:
            suggested_saleprice_competator = ""

        conn = pymysql.connect(host=self.rds_host,
                                user=self.name,
                                password=self.Hsql_password,
                                database=self.db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

        with conn.cursor() as cur:
                    sql="""INSERT INTO `[SchemaName]`.`srtpackage2` (`package`,`country`,`data`,`days`,`price`,`carrier_id`,`created_date`,`updated_date`,`activity_state`,`ranking`,`country_id`,`package_type`,`package_title`,`status`,
`suggested_saleprice`,`autoupdate`,`manual_saleprice`,`manual_validity_date_start`,`manual_validity_date_end`,`suggested_saleprice_criteria`,`suggested_saleprice_competator`)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
                    val = (package, country, data, days, price,carrier_id,created_date,updated_date,activity_state,ranking,country_id,package_type,package_title,status,suggested_saleprice,autoupdate,manual_saleprice,manual_validity_date_start,manual_validity_date_end,suggested_saleprice_criteria,suggested_saleprice_competator)
                    cur.execute(sql, val)
                    conn.commit()
        return cur.lastrowid


