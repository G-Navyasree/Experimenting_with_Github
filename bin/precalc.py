"""
DCM Precalc Extractor.
Created On: 26/4/2021
Created by : e44965
"""

from dateutil.parser import parse
from commons.ads.codesets import *
from state_utils import Utilities


class Loader(Utilities):
    """ Loader class"""

    def __init__(self, resource_manager, config, source_sys, app_logger):
        super().__init__(resource_manager, config)

        self.input = config['file']['pre_calc']
        self.interim = config['file']['interim']
        self.output = config['file']['output']
        self.file_data_list = []
        self.source_sys = source_sys
        self.app_logger = app_logger

        self.target_columns_dict = {}
        for i in self.precalc_target_columns:
            self.target_columns_dict[i] = ''

        self.output_list = []

        self.code_sets = Codesets.read_codesets(self.ads_conn, 103)
        self.codeset_jurisdiction = self.code_sets.get_codeset(name='Jurisdiction')
        self.codeset_comp_aff = self.code_sets.get_codeset(name='SCCMTransaction.AVCompAff')
        self.codeset_card_code = self.code_sets.get_codeset(name='SCCMTransaction.AVCardCode')

    def open_file_reader(self):
        """Read File"""
        self.app_logger.info("""Reading the input file """ )
        file = open(self.input)
        for i in file:
            if i[100:115].strip() != "" and "02X" not in i[25:28].strip() \
                and "AFB" not in i[130:145].strip() and "AFB" not in i[97:100]:
                self.file_data_list.append({'Transaction Type':'PreCalc',
                                            'Transaction Received Date': i[0:8].strip(),
                                            'Contract Number': i[130:145].strip(),
                                            'Card Code': i[63:68].strip(),
                                            'Duration': i[322:325].strip(),
                                            'Sales Team AP ID': i[100:115].strip(),
                                            'Application Signed Date': i[89:97].strip(),
                                            'Issue Date': i[81:89].strip(),
                                            'Jurisdiction': i[171:173].strip(),
                                            'Premium Effective Date': i[68:76].strip(),
                                            'Base Product Plan Code': i[145:155].strip(),
                                            'Commissionable Premium': i[202:218].strip(),
                                            'Adjustment Factor': i[279:287].strip(),
                                            'Share Percentage': i[315:322].strip(),
                                            'Insured Age': i[521:524].strip(),
                                            'Contract Name': {'firstname': i[471:521],
                                                                   'lastname': i[421:471]},
                                            'Company Affiliate Code': i[25:28].strip(),
                                            'Policy Carrier Code': i[25:28].strip(),
                                            'Commission Amount': i[187:203].strip(),
                                            'Commission Rate': i[271:279].strip()})
        
        self.app_logger.info("""Input File read successfully """ )
        return self.file_data_list

    def transformations(self):
        """ Handle Transformations """
        self.app_logger.info("""Applying transformations """ )
        for record in self.file_data_list:

            temp = self.target_columns_dict

            for field in record.keys():
                temp[field] = record[field]

                if field in self.date_field:
                    try:
                        dat = parse(record[field])
                        date = dat.strftime('%m/%d/%Y')
                        temp[field] = date
                    except ValueError:
                        temp[field] = "null"

                if field in self.strip_field:
                    if field == "Policy Carrier Code":
                        policy_no = record['Contract Number'][-3:]
                        temp[field] = self.policy_carrier_code[policy_no]
                    else:
                        temp[field] = record[field]

                if field in self.int_field:
                    temp[field] = int(record[field])

                if field in self.float_field:
                    if record[field]:
                        temp[field] = '{0:.2f}'.format(float(record[field]))
                    else:
                        temp[field] = 0

                if field in self.float_field_percentage:

                    temp[field] = '{0:.8f}'.format(float(record[field])*100)

                if field == "Contract Name":
                    name = record[field]['firstname'].strip()+" "+record[field]['lastname'].strip()
                    temp['Contract Name'] = name
                    
                if field == "Card Code":
                    card_code = self.card_code_opas[record[field]]
                    if self.codeset_card_code.get_short_desc(card_code) is not None:
                        temp[field] = card_code
                    else:
                        temp[field] = ""

                if field == "Jurisdiction" and \
                        self.codeset_jurisdiction.get_short_desc(record[field]) is not None:
                    temp[field] = record[field]

                if field == "Company Affiliate Code" and \
                        self.codeset_comp_aff.get_short_desc(record[field]) is not None:
                    temp[field] = record[field]

                policy_no = record['Contract Number'][-3:]
                temp["Carrier Admin System"] = self.carrier_admin_sys[policy_no]
                temp['Coverage Number'] = 1

            precise = Utilities.dcm_datatype_precalc_adjust(temp)
            self.output_list.append(dict(precise))

    def generate_output(self):
        """Generate output file"""
        self.app_logger.info("""Writing into the output file """ )
        file_append = ""

        file_append = open(self.output + "pre_calc.txt", 'w', newline='')

        header = "|".join(self.precalc_target_columns)

        file_append.write(header+"\n")

        for record in self.output_list:
            row = "|".join([str(item) for item in record.values()])
            file_append.write(row+"\n")
        file_append.write("Footer")
        file_append.close()
        self.app_logger.info("""Output file generated successfully """ )


def precalc_loader(resource_manager, config, source_sys, app_logger):
    """ Main loader function"""

    load = Loader(resource_manager, config, source_sys, app_logger)
    load.open_file_reader()
    load.transformations()
    load.generate_output()
