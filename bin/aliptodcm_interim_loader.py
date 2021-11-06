'''
ALIP to DCM Extractor.
Created On: 7/1/2021
Created by : e44972
'''

import csv
import datetime
from os.path import join
from postgres_functions import Table as T
from dateutil.parser import parse
from state_utils import Utilities
from commons.ads.codesets import *

SQL_TRANS_HISTORY_TABLE = '''SELECT
    LOADERCOMMTRANSKEY,
    TRANSACTIONTYPE,
    TRANSACTIONRECEIVEDDATE,
    POLICYNUMBER,
    CARDCODE,
    DURATION,
    INITIAL_PREMIUM_IND,
    AGREEMENTPARTICIPANTID,
    APPLICATIONSIGNEDDATE,
    ISSUEDATE,
    PREMIUMEFFECTIVEDATE,
    BASEPRODUCTPLANCODE,
    RIDERPRODUCTPLANCODE,
    JURISDICTION,
    COMMISSIONABLEPREMIUM,
    ADJUSTMENTFACTOR,
    SHAREPERCENTAGE,
    INSUREDAGE,
    POLICYHOLDERNAME,
    COMPANYAFFILIATECODE,
    CARRIERADMINSYSTEM,
    POLICYCARRIERCODE,
    RETAINEDCOMMISSIONAMOUNT,
    NETPREMIUMAMOUNT,
    TRANSFERSEQUENCENUMBER,
    TRANSFER_INITIATED,
    DATE_OF_DEATH,
    SERV_PROD_AP_ID,
    SERV_PROD_SHARE_PER,
    ADMIN_TRANSACTION_ID,
    PRE_ISSUE_TRANSFER,
    REVERSAL_IND,
    CONVERSIONDATE
FROM
    di_etl.trans_comm_history
WHERE
    1=1
ORDER BY LOADERCOMMTRANSKEY DESC
'''


class Loader(Utilities):
    '''It extracts the data from various tables using sql query and loads it into a text file'''

    def __init__(self, resource_manager, config, conf_file_name, app_logger, cycle_date):
        #Reading common parameters from Utilities __inint__
        super().__init__(resource_manager, config)
        self.aliptodcm_extract_file = config['file']['aliptodcm_extract']
        self.output_dir = config['DIRECTORIES']['output_dir'] + '/'
        self.output_file_name = config['file']['aliptodcm_premtrans']
        self.conf_file_name = conf_file_name
        self.app_logger = app_logger
        self.table_dm = None
        self.table_dm_ads = None
        self.fp = None
        self.alip_data_list = []
        self.dcm_data_list = []
        self.trans_history_data_list = []
        self.ads_card_code = []
        self.ads_jurisdiction = []
        self.ads_company_affiliate_code = []
        self.alip_data_dict = []
        self.incoming_data_fields = []
        self.cycle_date = cycle_date

        self.source_columns = ['TRANSACTIONRECEIVEDDATE', 'POLICYNUMBER', 'CARDCODE', 'DURATION',
                               'APPLICATIONSIGNEDDATE', 'ISSUEDATE', 'PREMIUMEFFECTIVEDATE',
                               'BASEPRODUCTPLANCODE', 'JURISDICTION', 'COMMISSIONABLEPREMIUM',
                               'ADJUSTMENTFACTOR', 'SHAREPERCENTAGE', 'INSUREDAGE', 'POLICYHOLDERNAME',
                               'COMPANYAFFILIATECODE', 'CARRIERADMINSYSTEM', 'POLICYCARRIERCODE',
                               'RETAINEDCOMMISSIONAMOUNT', 'NETPREMIUMAMOUNT', 'TRANSFERSEQUENCENUMBER',
                               'TRANSFER_INITIATED', 'DATE_OF_DEATH', 'ADMIN_TRANSACTION_ID', 'AGREEMENTPARTICIPANTID',
                                'SERV_PROD_AP_ID', 'SERV_PROD_SHARE_PER', 'PRE_ISSUE_TRANSFER', 'REVERSAL_IND',
                                'INITIAL_PREMIUM_IND', 'CONVERSIONDATE']
        self.two_prec = 2
        self.eight_prec = 8
        self.premium= 'Premium'
        self.source_target_dict = dict(zip(self.target_columns, self.source_columns))
        self.codesets=Codesets.read_codesets(self.ads_conn, 103)
        self.codeset_jurisdiction=self.codesets.get_codeset(name='Jurisdiction')
        self.codeset_comp_aff=self.codesets.get_codeset(name='SCCMTransaction.AVCompAff')
        self.codeset_card_code=self.codesets.get_codeset(name='SCCMTransaction.AVCardCode')
        self.output_file_writer()


    def output_file_writer(self):
        """Here output file is initialized"""
        self.fp = open(self.output_file_name, 'w')

    def output_file_writer_close(self):
        """Output file writer is closed"""
        if self.fp:
            self.fp.close()

    def generate_data_lists(self):
        """Generate list by extracting data from dcm_stage and alip extract"""
        self.alip_data_list = self.open_file_reader(self.aliptodcm_extract_file)
        self.incoming_data_fields = self.alip_data_list[0]
        del self.alip_data_list[0]

    @staticmethod
    def open_file_reader(file_name):
        """Read File"""
        file_data_list = []
        with open(file_name, 'r') as file:
            reader = csv.reader(file)
            file_data_list = list(reader)

        return file_data_list

    def load_alipdata_to_premtrans_interim(self):
        """ Loading the data in dcm_stage table from alip based on record differences """
        self.cursor_db_interface.execute(SQL_TRANS_HISTORY_TABLE)
        self.trans_history_data_list = self.cursor_db_interface.fetchone()
        insert_count = 0
        self.table_dm_ads = T(self.db_interface_conn, 'di_etl', 'trans_comm_history')
        stmt_ins_data_datainterface = self.table_dm_ads.get_insert_statement(self.db_interface_conn)

        if self.trans_history_data_list is not None:
            trans_history_table_record_count = int(self.trans_history_data_list[0]) + 1
        else:
            trans_history_table_record_count = 1

        #Header is written to output file
        print(self.target_header, file=self.fp)
        for alip_data in self.alip_data_list:
            temp_dict = dict(zip(self.incoming_data_fields, tuple(alip_data)))
            temp_dict.update({'LOADERCOMMTRANSKEY' : 'NULL'})
            incoming_data_dict, premtrans_data_dict = self.apply_transformations(temp_dict)
            premtrans_data_str = '|'.join([str(item) for item in premtrans_data_dict.values()])
               #Only sending PREMIUM transactions
            if temp_dict['TRANSACTIONTYPE'] == self.premium:
                print(premtrans_data_str, file=self.fp)

            if incoming_data_dict['CARDCODE'] != 1500010:
                incoming_data_dict.update({'LOADERCOMMTRANSKEY' : trans_history_table_record_count})
                stmt_ins_data_datainterface.persist(incoming_data_dict)
                trans_history_table_record_count = trans_history_table_record_count + 1

            insert_count = insert_count + 1

        self.output_file_writer_close()
        self.db_interface_conn.commit()
        self.cursor_db_interface.close()
        self.cursor_ads.close()
        self.app_logger.info("""aliptodcm_interim_loader alip data loaded to alip_premiumtransloader
                                and trans_comm_history table successfully.""")
        self.app_logger.info("""aliptodcm_interim_loader  No of record inserted : """ + str(insert_count))

        print("""aliptodcm_interim_loader alip data loaded to alip_premiumtransloader
                 and trans_comm_history table successfully.""")
        print('aliptodcm_interim_loader No of record inserted : ' + str(insert_count))

    def apply_transformations(self, data):
        """applying transformation on alip_extract to postgres table"""
        data_dict = {key:(value if value else '') for (key, value) in data.items()}

        data_dict['TRANSACTIONRECEIVEDDATE'] = data_dict['TRANSACTIONRECEIVEDDATE'].split(' ')[0] \
        if data_dict['TRANSACTIONRECEIVEDDATE'] else ""

        data_dict['TRANSACTIONTYPE'] = self.trans_type_dict[data_dict['TRANSACTIONTYPE']]  \
        if data_dict['TRANSACTIONTYPE'] else ''

        data_dict['CARDCODE'] = self.card_code_dict[data_dict['CARDCODE']] \
        if self.card_code_dict.get(data_dict['CARDCODE']) is not None else ''

        data_dict['POLICYHOLDERNAME'] = (data_dict['POLICYHOLDERNAME'][:50].strip()).replace("'", "") \
        if data_dict['POLICYHOLDERNAME'] != '' else ''

        data_dict['JURISDICTION'] = self.jurisdiction_dict[data_dict['JURISDICTION']] \
        if data_dict['JURISDICTION'] != '' else ''

        empty_column_list = ['POLICYNUMBER', 'BASEPRODUCTPLANCODE']

        for col in empty_column_list:
            data_dict[col] = data_dict[col].strip() if data_dict[col].strip() != '' else ''

        float_column_list = ['COMMISSIONABLEPREMIUM', 'ADJUSTMENTFACTOR', 'SHAREPERCENTAGE', 'RETAINEDCOMMISSIONAMOUNT']

        for col in float_column_list:
            data_dict[col] = float(data_dict[col]) if data_dict[col].strip() != '' else ''

        date_column_list = ['TRANSACTIONRECEIVEDDATE', 'APPLICATIONSIGNEDDATE', 'ISSUEDATE',
                            'PREMIUMEFFECTIVEDATE', 'TRANSFER_INITIATED', 'DATE_OF_DEATH',
                            'SERV_PROD_AP_ID','CONVERSIONDATE']

        for i in date_column_list:
            data_dict[i] = data_dict[i] if data_dict[i] != 'NULL' else str('')

        # Columns NOT coming from extract
        currentDT = datetime.datetime.now()
        showtime = currentDT.strftime("%Y-%m-%d %H:%M:%S")
        data_dict['CREATEDDATE'] = str(showtime)
        data_dict['LASTUPDATEDDATE'] = str(showtime)
        data_dict['CREATEDUSER'] = 'PyAdmin'
        data_dict['LASTUPDATEDUSER'] = 'PyAdmin'
        data_dict['COVERAGENUMBER'] = ''
        data_dict['POLICYPAYOUTDURATION'] = ''
        data_dict['COMMISSIONAMOUNT'] = ''
        data_dict['COMMISSIONRATE'] = ''
        data_dict['SOURCEENTITYNAME'] = ''
        data_dict['WRITING_PROD_AP_ID'] = ''
        data_dict['PRE_ISSUE_TRANSFER'] = data_dict['PRE_ISSUE_TRANSFER']
        data_dict['REVERSAL_IND'] = data_dict['REVERSAL_IND']
        data_dict['RIDERPRODUCTPLANCODE'] = ''

        number_data_list = ['TRANSACTIONTYPE', 'CARDCODE', 'DURATION', 'JURISDICTION',
                            'COMMISSIONABLEPREMIUM', 'ADJUSTMENTFACTOR', 'SHAREPERCENTAGE',
                            'INSUREDAGE', 'COVERAGENUMBER', 'POLICYPAYOUTDURATION',
                            'COMMISSIONAMOUNT', 'COMMISSIONRATE', 'RETAINEDCOMMISSIONAMOUNT',
                            'NETPREMIUMAMOUNT', 'SERV_PROD_SHARE_PER', 'ADMIN_TRANSACTION_ID',
                            'WRITING_PROD_AP_ID', 'COMPANYAFFILIATECODE']

        for field in number_data_list:
            data_dict[field] = 'NULL' if data_dict[field] == '' else data_dict[field]

        #applying transformation on alip_extract to postgres table
        out_data_dict = {key : data[self.source_target_dict[key]] for key in  self.source_target_dict.keys() if
                         self.source_target_dict[key]  in data_dict}

        out_data_dict['Transaction Received Date'] = out_data_dict['Transaction Received Date'].split(' ')[0] \
        if out_data_dict['Transaction Received Date'] else ''

        out_data_dict['Policy Holder Name'] = (out_data_dict['Policy Holder Name'][:50].strip()).replace("'", "") \
        if out_data_dict['Policy Holder Name'] != '' else ''

        if out_data_dict['Transfer Initiated'].strip() == '':
            out_data_dict['Transfer Initiated'] = 'Not Applicable'

        out_data_dict['Pre Issue Transfer'] = 'false' if out_data_dict['Pre Issue Transfer'].strip() == 'N' \
        else 'true'

        out_data_dict['Reversal'] = 'true' if out_data_dict['Reversal'].strip() == 'Y' else 'false'

        out_data_dict['Initial Premium'] = 'true' if out_data_dict['Initial Premium'].strip() == 'Y' \
        else 'false'

        if out_data_dict['Card Code'] == 'Freelook':
            out_data_dict['Card Code'] = 'Free Look'

        #verify cardcode, company_affiliate_code and jurisdiction against values from codeset
        out_data_dict['Card Code'] = out_data_dict['Card Code'] \
        if self.codeset_card_code.get_short_desc(out_data_dict['Card Code']) is not None else ''

        out_data_dict['Jurisdiction'] = out_data_dict['Jurisdiction'] \
        if  self.codeset_jurisdiction.get_short_desc(out_data_dict['Jurisdiction']) is not None else ''

        company_affiliate_code = self.company_affiliate_dict.get(out_data_dict['Company Affiliate Code'])
        out_data_dict['Company Affiliate Code'] = company_affiliate_code \
        if self.codeset_comp_aff.get_short_desc(company_affiliate_code)  is not None else ''

        #Transformations of empty handling
        out_empty_column_list = ['Policy Number', 'Base Product Plan Code']

        for i in out_empty_column_list:
            out_data_dict[i] = out_data_dict[i].strip() if out_data_dict[i].strip() != '' else ''

        #Transformation of amount related columns
        two_prec_data_list = ['Commissionable Premium', 'Retained Commission', 'Net Premium']
        eight_prec_data_list = ['Adjustment Factor', 'Writing Producer Share Percentage',
                                 'Servicing Producer Share Percentage']

        for column in two_prec_data_list:
            if out_data_dict[column].strip() != '':
                out_data_dict[column] = Utilities.round_off(out_data_dict[column], self.two_prec)
            else:
                out_data_dict[column] = 0

        for column in eight_prec_data_list:
            if out_data_dict[column].strip() != '':
                out_data_dict[column] = Utilities.round_off(out_data_dict[column], self.eight_prec)
            else:
                if column == 'Adjustment Factor':
                    out_data_dict[column] = 100
                else:
                    out_data_dict[column] = 0
            
        #suppress preceding zeros in admin transaction id
        out_data_dict['Admin Transaction ID'] = str(out_data_dict['Admin Transaction ID']).lstrip('0')
        #Date format conversion is perfomed
        out_date_column_list = ['Transaction Received Date', 'Application Signed Date', 'Issue Date',
                                'Premium Effective Date', 'Date Of Death', 'Conversion Date']

        for i in out_date_column_list:
            if out_data_dict[i].strip():
                try:
                    dt = parse(out_data_dict[i])
                    out_data_dict[i] = dt.strftime('%m/%d/%Y')
                except ValueError as error:
                    out_data_dict[i] = ''
                    self.app_logger.info("Error in aliptodcm_interim_loader apply_transformations")
                    self.app_logger.info('Invalid date column {} value {} '.format(i, out_data_dict[i]))
                    self.app_logger.info('Date Error '+str(error))
            else:
                out_data_dict[i] = ''

        #calling dcm_datatype_prec_adjust to change datatypes to DCM format
        final_data_dict = Utilities.dcm_datatype_prec_adjust(out_data_dict)

        return data_dict, final_data_dict

def alip_to_dcm_loader(resource_manager, config, conf_file_name, app_logger, cycle_date):
    '''Extract data from source table and load it into a csv file'''
    loader = Loader(resource_manager, config, conf_file_name, app_logger, cycle_date)
    loader.generate_data_lists()
    loader.load_alipdata_to_premtrans_interim()
