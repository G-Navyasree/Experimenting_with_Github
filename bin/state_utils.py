'''''
Common parameters for all sources.
Created On: 21/1/2021
Created by : e44972
'''


class Utilities():
    '''It contains all commmon parameters to be sued by all three source systems alip, opas and as400'''
    def __init__(self, resource_manager, config):
        db_ads = config['Resource-Links']['db']
        self.ads_conn = resource_manager.get_resource(db_ads)
        self.cursor_ads = self.ads_conn.cursor()
        db_interface = config['Resource-Links']['db_interface']
        self.db_interface_conn = resource_manager.get_resource(db_interface)
        self.cursor_db_interface = self.db_interface_conn.cursor()

        self.jurisdiction_dict = {'FS': 0, 'WA': 56, 'BC': 102, 'MI': 27, 'DE': 9, 'DC': 10, 'WI': 58, 'WV': 57,
                                 'HI': 15, 'WR': 0, 'ACT': 0, 'AE': 0, 'FL': 12, 'YT': 112, 'WY': 59, 'NH': 34,
                                 'SK': 111, 'NJ': 35, 'PQ': 110, 'LA': 22, 'TX': 51, 'TAS': 0, 'NB': 104, 'NC': 38,
                                 'ND': 39, 'NE': 32, 'NF': 105, 'TN': 50, 'NY': 37, 'PA': 45, 'PE': 109, 'NR': 0,
                                 'NS': 107, 'NT': 106, '$$': 0, 'NV': 33, 'NSW': 0, 'AA': 0, 'PR': 46, 'GU': 14,
                                 'AB': 101, 'CO': 7, 'PW': 0, 'VIC': 0, 'VI': 54, 'AK': 2, 'OH': 41, 'AL': 1, 'AS': 3,
                                 'AP': 0, 'VA': 55, 'AR': 5, 'VT': 53, 'NM': 36, 'GA': 13, 'IN': 18, 'IA': 19,
                                 'MA': 26, 'MD': 25, 'AZ': 4, 'CA': 6, 'ID': 16, 'SA': 0, 'CT': 8, 'ME': 23, 'ON': 108,
                                 'OK': 42, 'IL': 17, 'MB': 103, 'UT': 52, 'MO': 30, 'MN': 28, 'OC': 0, 'MH': 0,
                                 'OR': 43, 'RI': 47, 'KS': 20, 'MT': 31, 'MP': 40, 'MS': 29, 'SC': 48, 'KY': 21,
                                 'QL': 0, 'SD': 49,'NA':''}

        self.card_code_dict = {'First Year': 1, 'Renewal': 2, 'Trail': 1500006, 'Not Taken': 1500007,
                               'Surrender': 1500008, 'Advancing': 1500009, 'Advancing Chargeback': 1500010,
                               'WDOF': 1500011, 'Death': 1500012, 'Freelook': 1500013}

        self.card_code_as400 = {'1': 'First Year', '2': 'Renewal', '5': 'Trail', 'NT': 'Not Taken',
                                'SUR': 'Surrender', 'ADV': 'Advancing'}

        self.card_code_opas = {'1': 'First Year', '2': 'Renewal', '5': 'Trail', 'NT': 'Not Taken',
                                'SUR': 'Surrender', 'ADV': 'Advancing','WDOF': 'WDOF',
                               'ADVBK': 'Advancing Chargeback', 'Death': 'Death',
                               'Free look': 'Free look', 'Select One': 'Select One'}

        self.carrier_admin_sys = {'AAI': 'AS400AIL', 'ADA': 'AS400DLA',
                                  'ANX': 'AS400ANX', 'I10': 'AS400AML',
                                  'N01': 'AS400ILA'}

        self.date_field = ["Transaction Received Date", "Application Signed Date",
                      "Issue Date", "Premium Effective Date"]
        self.strip_field = ["Policy Number", "Base Product Plan Code", "Policy Carrier Code",
                       "Writing Producer AP ID", "Transfer Sequence Number"]
        self.int_field = ["Duration", "Issue Age", "Insured Age"]

        self.float_field = ["Commissionable Premium", "Retained Commission", "Net Premium", "Commission Amount"]

        self.float_field_percentage = ["Adjustment Factor", "Writing Producer Share Percentage", "Share Percentage"]

        self.policy_carrier_code = {'02B': 'O2B', 'AAI': 'AIA', 'ADA': 'DAA',
                                    'ANX': 'NXA', 'I10': '10I', 'N01': '01N'}

        self.trans_type_dict = {'Premium': 18}

        self.target_columns = ['Transaction Received Date', 'Policy Number', 'Card Code', 'Duration',
                               'Application Signed Date', 'Issue Date', 'Premium Effective Date',
                               'Base Product Plan Code', 'Jurisdiction', 'Commissionable Premium',
                               'Adjustment Factor', 'Writing Producer Share Percentage', 'Issue Age',
                               'Policy Holder Name', 'Company Affiliate Code', 'Carrier Admin System',
                               'Policy Carrier Code', 'Retained Commission', 'Net Premium', 'Transfer Sequence Number',
                                'Transfer Initiated', 'Date Of Death', 'Admin Transaction ID',
                                'Writing Producer AP ID', 'Servicing Producer AP ID',
                                'Servicing Producer Share Percentage', 'Pre Issue Transfer',
                                'Reversal', 'Initial Premium', 'Conversion Date']
                   
        self.precalc_target_columns = ['Transaction Type', 'Transaction Received Date', 'Contract Number',
                                        'Card Code', 'Duration', 'Sales Team AP ID',
                                        'Application Signed Date', 'Issue Date', 'Premium Effective Date',
                                        'Base Product Plan Code', 'Jurisdiction', 'Commissionable Premium',
                                        'Adjustment Factor', 'Share Percentage', 'Insured Age',
                                        'Contract Name', 'Company Affiliate Code', 'Carrier Admin System',
                                        'Policy Carrier Code','Coverage Number', 'Commission Amount',
                                        'Commission Rate', 'TransactionSequenceNo', 'Conversion Date']
                   
        self.target_header = '|'.join(self.target_columns)
        self.precalc_target_header = '|'.join(self.precalc_target_columns)
        self.company_affiliate_dict = {'1114200001':'10I', '1114200002':'01N', '1114200003' : 'AIA', '1114200004':'02B'}

    @staticmethod
    def round_off(value, precision = 2):
        """This function rounds off deimal to certain precision n as per oracle standards"""

        dot = '.'
        dec_prec = 1/(10**precision)
        #Rouding off values if its decimal are more than certain precision
        decimal_split = str(value).strip().split('.')
        if len(decimal_split) > 1:
            whole_num = decimal_split[0]
            dec_num = decimal_split[1]
            if len(dec_num) > precision:
                truncated_dec = dec_num[:precision+1]
                if int(str(truncated_dec)[-1]) >= 5:
                    updated_dec = str(truncated_dec[:precision])
                    round_dec = float(whole_num + dot + updated_dec)
                    if round_dec > 0:
                        round_dec = round_dec + dec_prec
                    else:
                        round_dec = round_dec - dec_prec  
                else:
                    updated_dec = str(truncated_dec[:precision])
                    round_dec = float(whole_num + dot + updated_dec)
                round_dec = float(round_dec)
                if round_dec == 0:
                    return abs(round_dec)
                else:
                    return round_dec
            else:
                value = float(value)
                if value == 0:
                    return abs(value)
                else:
                    return value
        else:
            value = float(value)
            if value == 0:
                return abs(value)
            else:
                return value


    @staticmethod
    def dcm_datatype_prec_adjust(data_dict):
        #This function changes the dataypes/precision of fields to DCM format
        datatype_dict = {'Transaction Received Date':{'String':10}, 'Policy Number':{'String':50},
                         'Card Code':{'String':100}, 'Duration':{'Number':38}, 'Application Signed Date':{'String':10},
                         'Issue Date':{'String':10}, 'Premium Effective Date':{'String':10},
                         'Base Product Plan Code':{'String':100}, 'Jurisdiction':{'String':100},
                         'Commissionable Premium':{'Decimal':2}, 'Adjustment Factor':{'Decimal':8},
                         'Writing Producer Share Percentage':{'Decimal':8}, 'Issue Age':{'Number':38},
                         'Policy Holder Name':{'String':50}, 'Company Affiliate Code':{'String':38},
                         'Carrier Admin System':{'String':50}, 'Policy Carrier Code':{'String':50},
                         'Retained Commission':{'Decimal':2}, 'Net Premium':{'Decimal':2},
                         'Transfer Sequence Number':{'String':50}, 'Transfer Initiated':{'String':50},
                         'Date Of Death':{'String':10}, 'Admin Transaction ID':{'Number':19},
                         'Writing Producer AP ID':{'String':5}, 'Servicing Producer AP ID':{'String':5},
                        'Servicing Producer Share Percentage':{'Decimal':8}, 'Pre Issue Transfer':{'String':5},
                        'Reversal':{'String':5}, 'Initial Premium':{'String':5}, 'Conversion Date':{'String':10}}

        for col in data_dict:

            if col in datatype_dict:
                datatype = datatype_dict[col]
                key, val = next(iter(datatype.items()))
                #Data manipulation for string and decimal             
                if  key == 'String':
                    data_dict[col] = data_dict[col][:val]
                elif key == 'Decimal':
                    try:
                        float_data = float(data_dict[col])
                        scale = '.' + str(val) + 'f'
                        data_dict[col] = format(float_data, scale)
                    except ValueError:
                        continue

        return data_dict
    
    @staticmethod
    def dcm_datatype_precalc_adjust(data_dict):
        #This function changes the dataypes/precision of fields to DCM format
        datatype_dict = {'Transaction Type': {'String':7},'Transaction Received Date':{'String':10}, 'Contract Number':{'String':50},
                         'Card Code':{'String':100}, 'Duration':{'Number':38},'Sales Team AP ID':{'String': 38}, 'Application Signed Date':{'String':10},
                         'Issue Date':{'String':10}, 'Premium Effective Date':{'String':10},
                         'Base Product Plan Code':{'String':100}, 'Jurisdiction':{'String':100},
                         'Commissionable Premium':{'Decimal':2}, 'Adjustment Factor':{'Decimal':8},
                         'Share Percentage':{'Decimal':8}, 'Insured Age':{'Number':38},
                         'Contract Name':{'String':50}, 'Company Affiliate Code':{'String':38},
                         'Carrier Admin System':{'String':50}, 'Policy Carrier Code':{'String':50},
                         'Coverage Number':{'Number':15}, 'Commission Amount':{'Decimal':2},
                         'Commission Rate':{'Decimal':8}, 'TransactionSequenceNo':{'Number':50},
                         'Conversion Date':{'String':10}}

        for col in data_dict:

            if col in datatype_dict:
                datatype = datatype_dict[col]
                key, val = next(iter(datatype.items()))
                #Data manipulation for string and decimal             
                if  key == 'String':
                    data_dict[col] = data_dict[col][:val]
                elif key == 'Decimal':
                    try:
                        float_data = float(data_dict[col])
                        scale = '.' + str(val) + 'f'
                        data_dict[col] = format(float_data, scale)
                    except ValueError:
                        continue

        return data_dict    

            