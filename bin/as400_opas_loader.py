"""
OPAS to DCM Extractor.
Created On: 2/3/2021
Created by : e45003
"""

from dateutil.parser import parse
from commons.ads.codesets import *
from state_utils import Utilities


class Loader(Utilities):
    """ Loader class"""

    def __init__(self, resource_manager, config, source_sys):
        super().__init__(resource_manager, config)

        if source_sys == "opas":
            self.input = config['file']['admn_prem']
        elif source_sys == "as400":
            self.input = config['file']['top_prem']

        self.interim = config['file']['interim']
        self.output = config['file']['output']
        self.file_data_list = []
        self.source_sys = source_sys

        self.target_columns_dict = {}
        for i in self.target_columns:
            self.target_columns_dict[i] = ''

        self.output_list = []

        self.code_sets = Codesets.read_codesets(self.ads_conn, 103)
        self.codeset_jurisdiction = self.code_sets.get_codeset(name='Jurisdiction')
        self.codeset_comp_aff = self.code_sets.get_codeset(name='SCCMTransaction.AVCompAff')
        self.codeset_card_code = self.code_sets.get_codeset(name='SCCMTransaction.AVCardCode')

    def open_file_reader(self):
        """Read File"""

        file = open(self.input)
        for i in file:
            if i[100:115].strip() != "" and "02X" not in i[25:28].strip() \
                    and "AFB" not in i[130:145].strip() and "AFB" not in i[97:100]:

                self.file_data_list.append({'Transaction Received Date': i[0:8].strip(),
                                            'Policy Number': i[130:145].strip(),
                                            'Card Code': i[63:68].strip(),
                                            'Duration': i[322:325].strip(),
                                            'Application Signed Date': i[89:97].strip(),
                                            'Issue Date': i[81:89].strip(),
                                            'Jurisdiction': i[171:173].strip(),
                                            'Premium Effective Date': i[68:76].strip(),
                                            'Base Product Plan Code': i[145:155].strip(),
                                            'Commissionable Premium': i[202:218].strip(),
                                            'Adjustment Factor': i[279:287].strip(),
                                            'Writing Producer Share Percentage': i[315:322].strip(),
                                            'Issue Age': i[521:524].strip(),
                                            'Policy Holder Name': {'firstname': i[471:521],
                                                                   'lastname': i[421:471]},
                                            'Company Affiliate Code': i[25:28].strip(),
                                            'Policy Carrier Code': i[25:28].strip(),
                                            'Retained Commission': i[594:610].strip(),
                                            'Net Premium': i[610:626].strip(),
                                            'Transfer Sequence Number': i[626:676].strip(),
                                            'Writing Producer AP ID': i[100:115].strip()})

        return self.file_data_list

    def transformations(self):
        """ Handle Transformations """

        for record in self.file_data_list:

            temp = self.target_columns_dict

            for field in record.keys():

                if field in self.date_field:
                    try:
                        dat = parse(record[field])
                        date = dat.strftime('%m/%d/%Y')
                        temp[field] = date
                    except ValueError:
                        temp[field] = "null"

                if field in self.strip_field:
                    if self.source_sys == "opas":
                        temp[field] = record[field]

                    elif self.source_sys == "as400":
                        if field == "Policy Carrier Code":
                            policy_no = record['Policy Number'][-3:]
                            temp[field] = self.policy_carrier_code[policy_no]
                        else:
                            temp[field] = record[field]

                if field in self.int_field:
                    temp[field] = int(record[field])

                if field in self.float_field:
                    if field == "Net Premium":
                        code_check = self.card_code_opas[record['Card Code']]
                        if self.source_sys == 'as400' or code_check == 'Trail':
                            temp[field] = '{0:.2f}'.format(float(record['Commissionable Premium']))
                        else:
                            if record[field]:
                                temp[field] = '{0:.2f}'.format(float(record[field]))
                            else:
                                temp[field] = 0

                    else:
                        if record[field]:
                            temp[field] = '{0:.2f}'.format(float(record[field]))
                        else:
                            temp[field] = 0

                if field in self.float_field_percentage:

                    temp[field] = '{0:.8f}'.format(float(record[field])*100)

                if field == "Policy Holder Name":

                    name = record[field]['firstname'].strip()+" "+record[field]['lastname'].strip()
                    temp['Policy Holder Name'] = name

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

                if self.source_sys == "opas":
                    temp["Carrier Admin System"] = "AdminServer"
                elif self.source_sys == "as400":

                    policy_no = record['Policy Number'][-3:]
                    temp["Carrier Admin System"] = self.carrier_admin_sys[policy_no]

                temp["Transfer Initiated"] = "Not Applicable"
                value = 0.0
                temp["Servicing Producer Share Percentage"] = '{0:.8f}'.format(value)

                temp["Pre Issue Transfer"] = "true"
                temp["Reversal"] = "false"
                temp["Initial Premium"] = "false"

            precise = Utilities.dcm_datatype_prec_adjust(temp)

            self.output_list.append(dict(precise))

    def generate_output(self):
        """Generate output file"""

        file_append = ""

        if self.source_sys == "opas":
            file_append = open(self.interim+"opas_premiumtransloader.txt", 'w', newline='')
        elif self.source_sys == "as400":
            file_append = open(self.interim + "as400_premiumtransloader.txt", 'w', newline='')

        header = "|".join(self.target_columns)

        file_append.write(header+"\n")

        for record in self.output_list:
            row = "|".join([str(item) for item in record.values()])
            file_append.write(row+"\n")

        file_append.close()


def loader(resource_manager, config, source_sys, app_logger):
    """ Main loader function"""

    load = Loader(resource_manager, config, source_sys)
    try:
        load.open_file_reader()
        print(source_sys, "File load success")
        app_logger.info(source_sys+"File load success")
    except Exception as e:
        app_logger.info(source_sys+"Error in loading input file %r",e)
        print(source_sys, "Error in loading input file %r", e)

    try:
        load.transformations()
        print(source_sys, "Transformation success")
        app_logger.info(source_sys+"Transformation success")
    except Exception as e:
        app_logger.info(source_sys+"Error in transformation function %r", e)
        print(source_sys, "Error in transformation function %r", e)

    try:
        load.generate_output()
        print(source_sys, "Output file generated successfully")
        app_logger.info(source_sys+"Output file generated successfully")
    except Exception as e:
        app_logger.info(source_sys+"Error in generating output file %r", e)
        print(source_sys, "Error in generating output file %r", e)

