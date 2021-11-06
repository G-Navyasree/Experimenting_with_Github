'''
Top_Basis Commission file Extractor

Created On:26/05/2021
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






class top_basis():
    '''It extracts the data from various tables using sql query and loads it into a text file'''
    def __init__(self, resource_manager, config, conf_file_name,app_logger, cycle_date,enviroment):
        db_alias = config['Resource-Links']['db']
        self.postgres_conn = resource_manager.get_resource(db_alias)
        self.cursor = self.postgres_conn.cursor()
        self.dcm_ads_to_basis_extract = config['file']['topbasis_extract']
        self.output_dir = config['DIRECTORIES']['output_dir'] + '/'
        self.temp_dir = config['DIRECTORIES']['temp_dir'] + '/'
        self.conf_file_name = conf_file_name
        self.app_logger=app_logger
        self.source_column = None
        self.count = 0
        sorting_writer_alias = config['Settings']['sorting_writer_alias']
        self.sorting_writer_factory = resource_manager.get_resource_factory(sorting_writer_alias)
        self.basis_data_list = []
        self.cycle_date =cycle_date
    def generate_topbasis_extract(self):
        """Generate Top_Basis commission extract"""
        self.load_top_basis_records_into_file()
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
    def load_top_basis_records_into_file (self):
        """Generate file from by extracting data from db"""
        self.app_logger.info('Extract for writing Top Basis file Started by executing the function load_top_basis records into file')
        query='''
        
         SELECT * FROM as400.commission_detail_history WHERE source_system_name ='TOPBASIS' and record_cycle_date='2021-06-30'
          '''
       
        self.extract_data_into_file(self.postgres_conn, query, 'topbasis_query_details.txt')
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
        """Generate Top Basis extract file after transformations"""
        basis_details_reader = self.open_file_reader('topbasis_query_details.txt')
        cur_date=str(datetime.now())
        cur_date=cur_date.replace("-","")
        cur_date=cur_date.replace(":","")
        if basis_details_reader != []:
            count=0
            for basis_details in basis_details_reader:
                basis_data = {}
                pad=[]
                count=count+1
                basis_data['Record Cycle Date']=cur_date[0:8]
                basis_data['Record Cycle Time']=cur_date[9:15]
                basis_data['Record ID'] = (str(count)).zfill(7)
                basis_data['Filler1']=''.ljust(2)
                basis_data['Policy_par_code']='N'
                basis_data['Filler2']=('').ljust(1)
                if (basis_details['company_code'])=='AIA':
                    basis_data['Company_code']=(basis_details['company_code']).rjust(3)
                elif(basis_details['company_code'])=='01N':
                    basis_data['Company_code']=(basis_details['company_code']).rjust(3)
                elif(basis_details['company_code'])=='10I':
                    basis_data['Company_code']=(basis_details['company_code']).rjust(3)
                elif(basis_details['company_code'])=='02B':
                    basis_data['Company_code']=(basis_details['company_code']).rjust(3)
                else:
                    pass
                basis_data['Generation Identifier']=(basis_details['generation_identifier']).ljust(8)
                basis_data['Filler3']=''.ljust(9)
                basis_data['Batch Number']='00000000'
                basis_data['Source System Name']=str(basis_details['source_system_name']).ljust(8)
                basis_data['Transaction Code']=str(basis_details['transaction_code']).ljust(2)
                basis_data['Card Code']=str(basis_details['card_code']).ljust(5)
                if(basis_details['transaction_effective_date']):
                    basis_data['Transaction Effective Date']=str((basis_details['transaction_effective_date']).replace('-',"")).ljust(8)
                else:
                    basis_data['Transaction Effective Date']=''.ljust(8)
                basis_data['Override Calculation Indicator']='N'
                basis_data['Rate Lookup Indicator']='N'
                basis_data['Annualized Premium Indicator']='N'
                basis_data['Wire Trade Indicator']=str(basis_details['wire_trade_indicator']).ljust(1)
                basis_data['initial_premium_indicator']='N'
                if(basis_details['coverage_issue_date']):
                    basis_data['coverage_issue_date']=(str(basis_details['coverage_issue_date']).replace("-","")).ljust(8)
                else:    
                    basis_data['coverage_issue_date']= basis_data['Transaction Effective Date']
                if(basis_details['application_signed_date']):
                    basis_data['Application Signed Date']=(str(basis_details['application_signed_date']).replace('-',"")).ljust(8)
                else:
                    basis_data['Application Signed Date']=basis_data['coverage_issue_date']
                basis_data['Agent_company_code']=str(basis_details['agent_company_code']).ljust(3)
                basis_data['Agent Number']=str(basis_details['agent_number']).ljust(15)
                basis_data['filler3']=''.ljust(15)
                basis_data['Policy Number']=str(basis_details['policy_number']).ljust(15) 
                basis_data['Base Coverage Plan Code']=str(basis_details['base_coverage_plan_code']).ljust(10)
                basis_data['Base Coverage Plan Rate Scale']=str(basis_details['base_coverage_plan_rate_scale']).ljust(1)
                basis_data['Filler4']=''.ljust(15)
                basis_data['Issue State']=str(basis_details['issue_state']).ljust(2)
                basis_data['Filler5']=''.ljust(5)
                basis_data['Issue Country']=str(basis_details['issue_country']).ljust(2)
                basis_data['Current State']=str(basis_details['current_state']).ljust(2)
                basis_data['Current Country']='US'
                basis_data['Currency Code']=str(basis_details['currency_code']).ljust(2)
                basis_data['Commission_Paid_Amount']=(str(basis_details['commission_paid_amount'])).zfill(15)
                pad.append(basis_data['Commission_Paid_Amount'])
                basis_data['Commissionable_Premium']=(str(basis_details['commissionable_premium'])).zfill(15)
                pad.append(basis_data['Commissionable_Premium'])
                basis_data['Commission_Earned']=(str(basis_details['commission_earned']).zfill(15))
                pad.append(basis_data['Commission_Earned'])
                basis_data['Target_Premium']=(str(basis_details['target_premium']).zfill(15))
                pad.append(basis_data['Target_Premium'])
                basis_data['Planned_Premium']=(str(basis_details['planned_premium']).zfill(15))
                pad.append(basis_data['Planned_Premium'])
                basis_data['Policy Count']=str(basis_details['policy_count']).rjust(5)
                basis_data['Commission Rate']=' 0.00000'
                basis_data['Commission Adjustment Factor']=float(basis_details['commission_adjustment_factor'])
                basis_data['Commission Adjustment Factor']="{0:.5f}".format(basis_data['Commission Adjustment Factor'])
                basis_data['Commission Adjustment Factor']=str(basis_data['Commission Adjustment Factor']).rjust(8)
                basis_data['Production_Premium']=(str(basis_details['production_premium']).zfill(15))
                pad.append(basis_data['Production_Premium'])
                basis_data['Annual Policy Fee']=(str(basis_details['annual_policy_fee']).zfill(11)).rjust(12)
                basis_data['Agent Share Percentage']=float(basis_details['agent_share_percentage'])
                basis_data['Agent Share Percentage']="{0:.5f}".format(basis_data['Agent Share Percentage'])
                basis_data['Agent Share Percentage']=str(basis_data['Agent Share Percentage']).rjust(7)
                basis_data['Policy Duration Years']=str(basis_details['policy_duration_years']).zfill(3)
                basis_data['Policy Duration Months']=str(basis_details['policy_duration_months']).zfill(3)
                basis_data['Coverage Plan Code']=str(basis_details['coverage_plan_code']).ljust(10)
                basis_data['Coverage Plan Rate Scale']=str(basis_details['coverage_plan_rate_scale']).ljust(1)
                basis_data['Coverage Number']=str(basis_details['coverage_number']).ljust(2)
                if(basis_details['coverage_premium_effective_date']):
                    basis_data['Coverage Premium Effective Date']=((str(basis_details['coverage_premium_effective_date'])).replace("-","")).ljust(8)
                else:
                    basis_data['Coverage Premium Effective Date']=''.ljust(8)
                basis_data['Coverage Status']=str(basis_details['coverage_status']).ljust(1)
                basis_data['Coverage Face Amount']=(str(basis_details['coverage_face_amount']).zfill(15)).rjust(16)
                basis_data['Policy Billing Mode']=str(basis_details['policy_billing_mode']).ljust(2)
                basis_data['Chargeback Rate']=float(basis_details['chargeback_rate'])
                basis_data['Chargeback Rate']="{0:.5f}".format(basis_data['Chargeback Rate'])
                basis_data['Chargeback Rate']=(str(basis_data['Chargeback Rate'])).rjust(8)
                basis_data['Element Code']="".ljust(5)
                basis_data['Fund Code']=''.ljust(10)
                if(basis_details['application_received_date']):
                    basis_data['Application Received Date']=((str(basis_details['application_received_date'])).replace("-","")).ljust(8)
                else:
                    basis_data['Application Received Date']=''.ljust(8)
                basis_data['GIC Term Months']=str(basis_details['gic_term_months']).zfill(3)
                basis_data['GIC Term Days']=str(basis_details['gic_term_days']).zfill(3)
                basis_data['Cumulative Deposit']=(str(basis_details['cumulative_deposit']).zfill(15)).rjust(16)
                basis_data['Insured Last Name']=str(basis_details['insured_last_name']).ljust(50)
                basis_data['Insured First Name']=str(basis_details['insured_first_name']).ljust(50)
                basis_data['Insured Age at Issue']=str(basis_details['insured_age_at_issue']).zfill(3)
                for element in range(0,6):   
                    if '-' in pad[element]:
                        pad[element]=(str(pad[element])).zfill(16)
                           
                    else:
                        pad[element]=(str(pad[element])).rjust(16)
                basis_data['Commission_Paid_Amount']=pad[0]
                basis_data['Commissionable_Premium']=pad[1]
                basis_data['Commission_Earned']=pad[2]
                basis_data['Target_Premium']=pad[3]
                basis_data['Planned_Premium']=pad[4]
                basis_data['Production_Premium']=pad[5]
                self.basis_data_list.append(basis_data)
            return self.basis_data_list
    def generate_target_file(self):
        '''Generate extract file in txt format '''
        with open(self.dcm_ads_to_basis_extract, 'w', newline='') as file_name:
            for basis_data_each in self.basis_data_list:
                data = basis_data_each.values()
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
            self.app_logger.info("Top Basis extract file generated successfully.")
def extractbasis(resource_manager, config, conf_file_name,app_logger, cycle_date, environment):
    '''Extract data from source table and load it into a txt file'''
    extractor =top_basis(resource_manager, config, conf_file_name,app_logger,cycle_date, environment)
    extractor.generate_topbasis_extract()
