'''
Top_Premium Commission file Extractor

Created On:25/05/2021
Updated On: 01/06/2021
Created by: e44961

This file extracts data from database and loads data into text file .
'''

import datetime
from datetime import date
from datetime import datetime
from os.path import join
from commons.dbutils.postgres import DataExporter
from commons.textfiles.delimited import DelimitedReader
from commons.sorting import SortedReader






class top_premium():
    '''It extracts the data from various tables using sql query and loads it into a text file'''
    def __init__(self, resource_manager, config, conf_file_name,app_logger, cycle_date,enviroment):
        db_alias = config['Resource-Links']['db']
        self.postgres_conn = resource_manager.get_resource(db_alias)
        self.cursor = self.postgres_conn.cursor()
        self.dcm_ads_to_premium_extract = config['file']['toppremium_extract']
        self.output_dir = config['DIRECTORIES']['output_dir'] + '/'
        self.temp_dir = config['DIRECTORIES']['temp_dir'] + '/'
        self.conf_file_name = conf_file_name
        self.app_logger=app_logger
        self.source_column = None
        self.count = 0
        sorting_writer_alias = config['Settings']['sorting_writer_alias']
        self.sorting_writer_factory = resource_manager.get_resource_factory(sorting_writer_alias)
        self.prem_data_list = []
        self.cycle_date =cycle_date
    def generate_toppremium_extract(self):
        """Generate Top_Premium commission extract"""
        self.load_top_prem_records_into_file()
        self.perform_transactions()
        self.generate_target_file()
    def extract_data_into_file(self, conn, sql, file_name):
        """Generate file from by extracting data from db"""
        file_name = open(join(self.temp_dir, file_name), 'wb')
        s_writer = self.sorting_writer_factory.create(dest=file_name, text_mode=False)
        exporter = DataExporter(conn, sql)
        exporter.create_meta()
        meta_header = exporter.get_formatted_meta()
        file_name.write(meta_header.encode('utf-8'))
        file_name.write(b'\n')
        exporter.export_using_copy_to(s_writer)                               
    def load_top_prem_records_into_file (self):
        """Generate file from by extracting data from db"""
        self.app_logger.info('Extract for writing Top Premium file Started by executing the function load_top_prem records into file')
        query='''
        
          SELECT * FROM as400.commission_detail WHERE source_system_name ='TOPPREM'
          '''
        self.extract_data_into_file(self.postgres_conn, query, 'toppremium_query_details.txt')
        self.postgres_conn.close()
    def open_file_reader(self, file_name, meta_in_header=True, \
                          meta_type='json', parse_type=2, is_sorted=False, delimiter='\t'):
        """Read File"""
        file = open(join(self.temp_dir, file_name), 'r')
        reader = DelimitedReader(file, meta_in_header=meta_in_header, \
                                  meta_type=meta_type, parse_type=parse_type, delimiter=delimiter)
        if is_sorted:
            return SortedReader(reader, enforce_strict_ordering=True)
        return reader
    def perform_transactions(self):
        """Generate Top Premium extract file after transformations"""
        prem_details_reader = self.open_file_reader('toppremium_query_details.txt')
        cur_date=str(datetime.now())
        cur_date=cur_date.replace("-","")
        cur_date=cur_date.replace(":","")
        if prem_details_reader != []:
            count=0
            for prem_details in prem_details_reader:
                prem_data = {}
                pad=[]
                count=count+1
                prem_data['Record Cycle Date']=cur_date[0:8]
                prem_data['Record Cycle Time']=cur_date[9:15]
                prem_data['Record ID'] = (str(count)).zfill(7)
                prem_data['Filler1']=''.ljust(2)
                prem_data['Policy_par_code']='N'
                prem_data['Filler2']=('').ljust(1)
                if (prem_details['company_code'])=='AIA':
                    prem_data['Company_code']=(prem_details['company_code']).rjust(3)
                elif(prem_details['company_code'])=='01N':
                    prem_data['Company_code']=(prem_details['company_code']).rjust(3)
                elif(prem_details['company_code'])=='10I':
                    prem_data['Company_code']=(prem_details['company_code']).rjust(3)
                elif(prem_details['company_code'])=='02B':
                    prem_data['Company_code']=(prem_details['company_code']).rjust(3)
                else:
                    pass
                prem_data['Generation Identifier']=(prem_details['generation_identifier']).ljust(8)
                prem_data['Filler3']=''.ljust(9)
                prem_data['Batch Number']='00000000'
                prem_data['Source System Name']=str(prem_details['source_system_name']).ljust(8)
                prem_data['Transaction Code']=str(prem_details['transaction_code']).ljust(2)
                prem_data['Card Code']=str(prem_details['card_code']).ljust(5)
                if(prem_details['transaction_effective_date']):
                    prem_data['Transaction Effective Date']=str((prem_details['transaction_effective_date']).replace('-',"")).ljust(8)
                else:
                    prem_data['Transaction Effective Date']=''.ljust(8)
                prem_data['Override Calculation Indicator']='Y'
                prem_data['Rate Lookup Indicator']='Y'
                prem_data['Annualized Premium Indicator']='N'
                prem_data['Wire Trade Indicator']=str(prem_details['wire_trade_indicator']).ljust(1)
                prem_data['initial_premium_indicator']=str(prem_details['initial_premium_indicator']).ljust(1)
                if(prem_details['coverage_issue_date']):
                    prem_data['coverage_issue_date']=(str(prem_details['coverage_issue_date']).replace("-","")).ljust(8)
                else:    
                    prem_data['coverage_issue_date']=prem_data['Transaction Effective Date']
                if(prem_details['application_signed_date']):
                    prem_data['Application Signed Date']=(str(prem_details['application_signed_date']).replace('-',"")).ljust(8)
                else:
                    prem_data['Application Signed Date']=prem_data['coverage_issue_date']   
                prem_data['Agent_company_code']=str(prem_details['agent_company_code']).ljust(3)
                prem_data['Agent Number']=str(prem_details['agent_number']).ljust(15)
                prem_data['filler3']=''.ljust(15)
                prem_data['Policy Number']=str(prem_details['policy_number'] ).ljust(15)
                prem_data['Base Coverage Plan Code']=str(prem_details['base_coverage_plan_code']).ljust(10)
                prem_data['Base Coverage Plan Rate Scale']=str(prem_details['base_coverage_plan_rate_scale']).ljust(1)
                prem_data['Filler4']=''.ljust(15)
                prem_data['Issue State']=str(prem_details['issue_state']).ljust(2)
                prem_data['Filler5']=''.ljust(5)
                prem_data['Issue Country']=str(prem_details['issue_country']).ljust(2)
                prem_data['Current State']=str(prem_details['current_state']).ljust(2)
                prem_data['Current Country']='US'
                prem_data['Currency Code']=str(prem_details['currency_code']).ljust(2)
                prem_data['Commission_Paid_Amount']=(str(prem_details['commission_paid_amount']).zfill(15))
                pad.append(prem_data['Commission_Paid_Amount'])
                prem_data['Commissionable_Premium']=(str(prem_details['commissionable_premium']).zfill(15))
                pad.append(prem_data['Commissionable_Premium'])
                prem_data['Commission Earned']=(str(prem_details['commission_earned']).zfill(15))
                pad.append(prem_data['Commission Earned'])
                prem_data['Target Premium']=(str(prem_details['target_premium']).zfill(15))
                pad.append(prem_data['Target Premium'])
                prem_data['Planned Premium']=(str(prem_details['planned_premium']).zfill(15))
                pad.append(prem_data['Planned Premium'])
                prem_data['Policy Count']=str(prem_details['policy_count']).rjust(5)
                prem_data['Commission Rate']=' 0.00000'
                prem_data['Commission Adjustment Factor']=float(prem_details['commission_adjustment_factor'])
                prem_data['Commission Adjustment Factor']="{0:.5f}".format(prem_data['Commission Adjustment Factor'])
                prem_data['Commission Adjustment Factor']=str(prem_data['Commission Adjustment Factor']).rjust(8)
                prem_data['Production Premium']=(str(prem_details['production_premium']).zfill(15))
                pad.append(prem_data['Production Premium'])
                prem_data['AnnualPolicyFee']=(str(prem_details['annual_policy_fee']).zfill(11)).rjust(12) 
                prem_data['Agent Share Percentage']=float(prem_details['agent_share_percentage'])
                prem_data['Agent Share Percentage']="{0:.5f}".format(prem_data['Agent Share Percentage'])
                prem_data['Agent Share Percentage']=str(prem_data['Agent Share Percentage']).rjust(7)
                prem_data['Policy Duration Years']=str(prem_details['policy_duration_years']).zfill(3)
                prem_data['Policy Duration Months']=str(prem_details['policy_duration_months']).zfill(3)
                prem_data['Coverage Plan Code']=str(prem_details['coverage_plan_code']).ljust(10)
                prem_data['Coverage Plan Rate Scale']=str(prem_details['coverage_plan_rate_scale']).ljust(1)
                prem_data['Coverage Number']=str(prem_details['coverage_number']).ljust(2)
                if(prem_details['coverage_premium_effective_date']):
                    prem_data['Coverage Premium Effective Date']=((str(prem_details['coverage_premium_effective_date'])).replace("-","")).ljust(8)
                else:
                    prem_data['Coverage Premium Effective Date']=''.ljust(8)
                prem_data['Coverage Status']=str(prem_details['coverage_status']).ljust(1)
                prem_data['Coverage Face Amount']=(str(prem_details['coverage_face_amount']).zfill(15)).rjust(16)
                prem_data['Policy Billing Mode']=str(prem_details['policy_billing_mode']).ljust(2)
                prem_data['Chargeback Rate']=float(prem_details['chargeback_rate'])
                prem_data['Chargeback Rate']="{0:.5f}".format(prem_data['Chargeback Rate'])
                prem_data['Chargeback Rate']=(str(prem_data['Chargeback Rate'])).rjust(8)
                prem_data['Element Code']="".ljust(5)
                prem_data['Fund Code']=''.ljust(10)
                if(prem_details['application_received_date']):
                    prem_data['Application Received Date']=((str(prem_details['application_received_date'])).replace("-","")).ljust(8)
                else:
                    prem_data['Application Received Date']=''.ljust(8)
                prem_data['GIC Term Months']=str(prem_details['gic_term_months']).zfill(3)
                prem_data['GIC Term Days']=str(prem_details['gic_term_days']).zfill(3)
                prem_data['Cumulative Deposit']=(str(prem_details['cumulative_deposit']).zfill(15)).rjust(16)
                prem_data['Insured Last Name']=str(prem_details['insured_last_name']).ljust(50)
                prem_data['Insured First Name']=str(prem_details['insured_first_name']).ljust(50)
                prem_data['Insured Age at Issue']=str(prem_details['insured_age_at_issue']).zfill(3)
                for element in range(0,6):   
                    if '-' in pad[element]:
                        pad[element]=(str(pad[element])).zfill(16)  
                    else:
                        pad[element]=(str(pad[element])).rjust(16)
                prem_data['Commission_Paid_Amount']=pad[0]
                prem_data['Commissionable_Premium']=pad[1]
                prem_data['Commission Earned']=pad[2]
                prem_data['Target Premium']=pad[3]
                prem_data['Planned Premium']=pad[4]
                prem_data['Production Premium']=pad[5]
                self.prem_data_list.append(prem_data)
            return self.prem_data_list
    def generate_target_file(self):
        '''Generate extract file in txt format '''
        with open(self.dcm_ads_to_premium_extract, 'w', newline='') as file_name:
            for prem_data_each in self.prem_data_list:
                data = prem_data_each.values()
                row=""
                for key_values in data:
                    row=row+str(key_values)
                l=len(row)
                if(l!=524):
                    val=abs(l-524)
                    if "None" in row:
                        row=row.replace("None","".rjust(val))
                file_name.write(row)
                file_name.write("\n")
            self.app_logger.info("Top premium extract file generated successfully.")
def extractpremium(resource_manager, config, conf_file_name,app_logger, cycle_date, environment):
    '''Extract data from source table and load it into a txt file'''
    extractor =top_premium(resource_manager, config, conf_file_name,app_logger,cycle_date, environment)
    extractor.generate_toppremium_extract()
