"""
Developer Signature: Kashif Sattar — ESIM Operator Plan Ranker
Purpose: Business processing and ranking workflow layer.
Public Repo Note: Credentials, database names, schema names, and SSH key paths must come from environment variables.
"""

import time
from dpl.dplinit import Connector
import paramiko
from sshtunnel import SSHTunnelForwarder
from Config.global_config import Config
import pymysql

class BPLInit:

    """Business processing layer for package ranking and persistence."""
    def __init__(self):
        """Initialize database, SSH, and runtime configuration values for this component."""
        self.rds_host  = Config.HOST_NAME
        self.name = Config.USER_NAME
        self.password = Config.PASSWORD
        self.db_name = Config.DB
        self.sql_hostname = Config.HOST_NAME
        self.sql_username = Config.USER_NAME
        self.sql_password = Config.PASSWORD
        self.sql_main_database = Config.DB
        self.sql_port = 3306
        self.ssh_host = Config.ssh_host
        self.ssh_user = Config.ssh_user
        self.ssh_port = Config.ssh_port
        self.sql_ip = Config.sql_ip
        pkeyfilepath = Config.SSH_PRIVATE_KEY_PATH
        self.mypkey = paramiko.RSAKey.from_private_key_file(pkeyfilepath) if pkeyfilepath else None


    def get_records_function_V1(self):

        """Run the direct-database ranking workflow for all configured countries, data bundles, and durations."""
        connector = Connector()

        lstcountry = self.get_country_V1()
        lstdata = self.get_data_V1()  # Moved inside the loop
        lstdays = self.get_days_V1()


        if not lstcountry:
            return []
        for country in lstcountry:

            for data in lstdata:

                if data == 'Unlimited':

                  for days in lstdays:

                    arr_records = connector.fetch_records_array_V1(country, data, days)

                    if not arr_records:
                        continue

                    srt_records = self.pkg_ranker(arr_records)
                    proc_record = self.proc_record_val(srt_records)
                    print(f"Sorted records for {country}: {srt_records}")
                    SSP = 0  # Example value, replace with actual value
                    AutoUpdate = True  # Example value, replace with actual value
                    MSP = 0 # Example value, replace with actual value
                    MVSD = None  # Example value, replace with actual value
                    MVED = None  # Example value, replace with actual value
                    SSC = ''  # Example value, replace with actual value
                    SSComp = ''  # Example value, replace with actual value
                    self.save_records_array_V1(proc_record,SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)
                    time.sleep(5)
                    print("Save Complete")

                else:

                    arr_records = connector.fetch_records_array_V1(country, data, "")

                    if not arr_records:
                        continue

                    arr_package_names = [record["package_name"] for record in arr_records]

                    srt_records = self.pkg_ranker(arr_records)
                    proc_record = self.proc_record_val(srt_records)
                    print(f"Sorted records for {country}: {srt_records}")
                    SSP = 0  # Example value, replace with actual value
                    AutoUpdate = True  # Example value, replace with actual value
                    MSP = 0 # Example value, replace with actual value
                    MVSD = None  # Example value, replace with actual value
                    MVED = None  # Example value, replace with actual value
                    SSC = ''  # Example value, replace with actual value
                    SSComp = ''  # Example value, replace with actual value
                    self.save_records_array_V1(proc_record,SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)
                    time.sleep(5)
                    print("Save Complete")



        return True

    def get_records_function_ssh_WDays(self):

        """Run the SSH-tunnel ranking workflow, including duration-aware unlimited plans."""
        connector = Connector()
        lstcountry = self.get_country_via_ssh()
        lstdata = self.get_data_via_ssh()  # Moved inside the loop
        lstdays = self.get_days_via_ssh()


        with SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_pkey=self.mypkey,
            remote_bind_address=(self.sql_hostname, self.sql_port)) as tunnel:
                conn = pymysql.connect(host='127.0.0.1', user=self.sql_username,
                passwd=self.sql_password, db=self.sql_main_database,
                port=tunnel.local_bind_port)
                count=0
                if not lstcountry:
                    return []
                for country in lstcountry:

                    for data in lstdata:

                            if data == 'Unlimited':

                                for days in lstdays:


                                    arr_records = connector.fetch_records_array_ssh_WDays2(conn,country, data, days)
                                    if not arr_records:
                                        continue
                                    srt_records = self.pkg_ranker(arr_records)
                                    print(f"Sorted records for {country}: {srt_records}")
                                    SSP = 0  # Example value, replace with actual value
                                    AutoUpdate = True  # Example value, replace with actual value
                                    MSP = 0 # Example value, replace with actual value
                                    MVSD = '0000-00-00 00:00:00'  # Example value, replace with actual value
                                    MVED = '0000-00-00 00:00:00'  # Example value, replace with actual value
                                    SSC = ''  # Example value, replace with actual value
                                    SSComp = ''  # Example value, replace with actual value
                                    self.save_records_array_ssh_WDays(conn,srt_records,SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)

                            else:



                                    arr_records = connector.fetch_records_array_ssh_WDays(conn,country, data)
                                    if not arr_records:
                                        continue
                                    srt_records = self.pkg_ranker(arr_records)
                                    print(f"Sorted records for {country}: {srt_records}")
                                    SSP = 0
                                    AutoUpdate = True  # Example value, replace with actual value
                                    MSP = 0 # Example value, replace with actual value
                                    MVSD = '0000-00-00 00:00:00'  # Example value, replace with actual value
                                    MVED = '0000-00-00 00:00:00'  # Example value, replace with actual value
                                    SSC = ''  # Example value, replace with actual value
                                    SSComp = ''  # Example value, replace with actual value
                                    self.save_records_array_ssh_WDays(conn,srt_records,SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)



                            count+=1



                conn.close()



        return True
    def get_records_function_ssh(self):

        """Run the SSH-tunnel ranking workflow for configured countries and data bundles."""
        connector = Connector()
        lstcountry = self.get_country_via_ssh()
        lstdata = self.get_data_via_ssh()  # Moved inside the loop
        lstdays = self.get_days_via_ssh()


        with SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_pkey=self.mypkey,
            remote_bind_address=(self.sql_hostname, self.sql_port)) as tunnel:
                conn = pymysql.connect(host='127.0.0.1', user=self.sql_username,
                passwd=self.sql_password, db=self.sql_main_database,
                port=tunnel.local_bind_port)
                count=0
                if not lstcountry:
                    return []
                for country in lstcountry:

                    for data in lstdata:

                        for days in lstdays:

                            arr_records = connector.fetch_records_array_ssh(conn,country, data, days)
                            if not arr_records:
                                continue
                            srt_records = self.pkg_ranker(arr_records)
                            print(f"Sorted records for {country}: {srt_records}")
                            SSP = 0  # Example value, replace with actual value
                            AutoUpdate = True  # Example value, replace with actual value
                            MSP = 0 # Example value, replace with actual value
                            MVSD = '0000-00-00 00:00:00'  # Example value, replace with actual value
                            MVED = '0000-00-00 00:00:00'  # Example value, replace with actual value
                            SSC = ''  # Example value, replace with actual value
                            SSComp = ''  # Example value, replace with actual value
                            self.save_records_array_ssh(conn,srt_records,SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)

                            arr_StrRecords = connector.fetch_records_array_StrPackage_ssh(conn,country, data, days)
                            if arr_StrRecords:
                                for str_record in arr_StrRecords:
                                    if str_record not in arr_records:
                                        print(f"Record not in arr_records: {str_record}")

                            count+=1



                conn.close()



        return True


    def get_country_via_ssh(self):
        """Fetch the list of country codes used as ranking input."""
        connector = Connector()
        arr_records = connector.fetch_country_via_ssh()
        return arr_records

    def get_data_via_ssh(self):
        """Fetch the available data bundle values used as ranking input."""
        connector = Connector()
        arr_records = connector.fetch_data_via_ssh()
        return arr_records

    def get_days_via_ssh(self):
        """Fetch available package duration values used as ranking input."""
        connector = Connector()
        arr_records = connector.fetch_days_via_ssh()
        return arr_records


    def get_data(self):
        """Fetch the available data bundle values used as ranking input."""
        connector = Connector()
        arr_data = connector.fetch_pkg_data()
        return arr_data

    def get_days(self):
        """Fetch available package duration values used as ranking input."""
        connector = Connector()
        arr_days = connector.fetch_pkg_days()
        return arr_days

    def get_country(self):
        """Fetch the list of country codes used as ranking input."""
        connector = Connector()
        arr_records = connector.fetch_country()
        return arr_records


    def manual_sort_records(self, records, column):
        """Sort records manually using the configured price field."""
        if not records:
            return []

        if column not in records[0]:
            raise ValueError(f"Column '{column}' does not exist in records")

        for i in range(len(records)):
            for j in range(i + 1, len(records)):
                if records[i][column] > records[j][column]:
                    records[i], records[j] = records[j], records[i]

        return records

    def validate_record_ssh(self,conn ,value):
        """Check whether a package/ranked record already exists before insert or update."""
        connector = Connector()
        result = connector.validate_record_ssh(conn,value)
        return result

    def validate_record_ssh_WDays(self,conn ,value):
        """Check whether a package/ranked record already exists before insert or update."""
        connector = Connector()
        result = connector.validate_record_ssh_WDays(conn,value)
        return result

    def validate_record(self, value):
        """Check whether a package/ranked record already exists before insert or update."""
        connector = Connector()
        result = connector.validate_record(value)
        return result

    def validate_record_V1(self, value):
        """Check whether a package/ranked record already exists before insert or update."""
        connector = Connector()
        result = connector.validate_record_V1(value)
        return result

    def pkg_ranker(self, records):
        """Rank package records by normalized net price and return the most cost-effective ordering."""
        column = "net_price"
        if not records:
            return []

        if column not in records[0]:
            raise ValueError(f"Column '{column}' does not exist in records")

        n = len(records)

        for i in range(n):
            for j in range(0, n-i-1):

                if records[j][column] > records[j+1][column]:

                    records[j], records[j+1] = records[j+1], records[j]

        return records

    def proc_record_val(self, records):

        """Prepare ranked records for database persistence by assigning sort/rank metadata."""
        column = "rank"
        if not records:
            return []

        if column not in records[0]:
            raise ValueError(f"Column '{column}' does not exist in records")
        i=1
        for record in records:
            record[column] = i
            i+=1

        return records
    def save_records_array(self, records):
        """Persist an array of processed ranked package records."""
        if not records:
            return False

        for record in records:
            self.save_srtpackage(record)

        return True

    def save_records_array_ssh(self, conn, records,SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp):

        """Persist an array of processed ranked package records."""
        if not records:
            return False
        rank = 0
        for record in records:
            result = self.validate_record_ssh(conn,record["package_name"])
            connector = Connector()
            if not result:
                rank+=1


                connector.insert_srtpackage_via_ssh(conn,
                record["package_name"], record["countries"],record["data_text"], record["days"],record["net_price"],record["carrier_id"],
                record["created_at"],record["updated_at"],record["action_state"],rank,record["country_id"],record["package_type"],
                record["title"],record["package_status"],SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)

            else:

                connector.update_srtpackage_via_ssh(conn,
                record["package_name"], record["countries"],record["data_text"], record["days"],record["net_price"],record["carrier_id"],
                record["created_at"],record["updated_at"],record["action_state"],rank,record["country_id"],record["package_type"],
                record["title"],record["package_status"],SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)

    def save_records_array_ssh_WDays(self, conn, records,SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp):

        """Persist an array of processed ranked package records."""
        if not records:
            return False
        rank = 0
        for record in records:
            result = self.validate_record_ssh_WDays(conn,record["package_name"])
            connector = Connector()
            if not result:
                rank+=1


                connector.insert_srtpackage2_via_ssh(conn,
                record["package_name"], record["countries"],record["data_text"], record["days"],record["net_price"],record["carrier_id"],
                record["created_at"],record["updated_at"],record["action_state"],rank,record["country_id"],record["package_type"],
                record["title"],record["package_status"],SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)

            else:
                rank+=1
                connector.update_srtpackage2_via_ssh(conn,
                record["package_name"], record["countries"],record["data_text"], record["days"],record["net_price"],record["carrier_id"],
                record["created_at"],record["updated_at"],record["action_state"],rank,record["country_id"],record["package_type"],
                record["title"],record["package_status"],SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)


    def get_country_V1(self):
        """Fetch the list of country codes used as ranking input."""
        connector = Connector()
        arr_records = connector.fetch_country_V1()
        return arr_records

    def get_data_V1(self):
        """Fetch the available data bundle values used as ranking input."""
        connector = Connector()
        arr_records = connector.fetch_data_V1()
        return arr_records

    def get_days_V1(self):
        """Fetch available package duration values used as ranking input."""
        connector = Connector()
        arr_records = connector.fetch_days_V1()
        return arr_records

    def save_records_array_V1(self,records,SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp):

        """Persist an array of processed ranked package records."""
        if not records:
            return False
        rank = 0
        for record in records:
            result = self.validate_record_V1(record["package_name"])
            connector = Connector()
            if not result:


                connector.insert_srtpackage_v1(
                record["package_name"], record["countries"],record["data_text"], record["days"],record["net_price"],record["carrier_id"],
                record["created_at"],record["updated_at"],record["action_state"],record["rank"],record["country_id"],record["package_type"],
                record["title"],record["package_status"],SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)

            else:

                connector.update_srtpackage_v1(
                record["package_name"], record["countries"],record["data_text"], record["days"],record["net_price"],record["carrier_id"],
                record["created_at"],record["updated_at"],record["action_state"],record["rank"],record["country_id"],record["package_type"],
                record["title"],record["package_status"],SSP,AutoUpdate,MSP,MVSD,MVED,SSC,SSComp)

    def update_srtpackage_status_v1(self,record,status):

        """Update the status value for a sorted package record."""
        connector = Connector()
        connector.update_srtpackage_status_v1(record,status)


    def save_srtpackage_ssh(self, conn,record):

        """Persist the selected sorted package record."""
        result = self.validate_record_ssh(conn,record["package_name"])

        if not result:
            connector = Connector()
            connector.insert_srtpackage_via_ssh(conn,record["package_name"], record["countries"],
                                    record["data_text"], record["days"],
                                    record["net_price"])
            return True
        else:
           return False

    def save_srtpackage(self, record):

        """Persist the selected sorted package record."""
        result = self.validate_record(record["package_name"])

        if not result:
            connector = Connector()
            connector.insert_srtpackage(record["package_name"], record["countries"],
                                    record["data_text"], record["days"],
                                    record["net_price"])
            return True
        else:
           return False


    def update_record(self, records):
        """Update existing package records in the configured database table."""
        connector = Connector()
        connector.update_records(records)
        return records

