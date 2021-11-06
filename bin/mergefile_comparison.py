''' Version 2 direct compare between the old and new files'''

import argparse
import configparser
from os.path import join

import csv
from itertools import zip_longest


class Comparison():
    '''This script compares the new and old premium trans loader files'''
    def __init__(self, config):
        self.merge_loader_premtrans_file = config['file']['merge_loader_premtrans']
        self.dcm_loader_premtrans_file = config['file']['dcm_loader_premtrans']
        self.output_dir = config['DIRECTORIES']['output_dir'] + '/'
        self.diff_output_file = config['file']['diff_output']

    def files_comparison_v2(self):
        '''FIles are read, sorted and line to line comparison is done and difference file is created'''
        merge_file = open(self.merge_loader_premtrans_file)
        dcm_file = open(self.dcm_loader_premtrans_file)
        content1, content2 = merge_file.read(), dcm_file.read()
        merge_list, dcm_list = content1.split("\n"), content2.split("\n")
        if '' in merge_list:
            merge_list.remove('')
        if '' in dcm_list:
            dcm_list.remove('')
        merge_list, dcm_list = [i.split('|') for i in merge_list], [j.split('|') for j in dcm_list]
        # Remove Headers
        merge_list.pop(0)
        dcm_list.pop(0)
        #Remove footer
        merge_list.pop(len(merge_list)-1)
        dcm_list.pop(len(dcm_list)-1)
        print(type(merge_list))

        # Sort the two files in the same order for comparison line by line:
        merge_list = sorted(merge_list, key=lambda x : (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10],
                                                     x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18],
                                                     x[19], x[20], x[21], x[22], x[23], x[24], x[25], x[26], x[27]))
        dcm_list = sorted(dcm_list, key=lambda x : (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10],
                                                  x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18], x[19],
                                                  x[20], x[21], x[22], x[23], x[24], x[25], x[26], x[27]))

        # Write the difference into two lists:
        merge_list_diff, dcm_list_diff = [], []
        for _, (i, j) in enumerate(zip(merge_list, dcm_list)):
            if i != j:
                merge_list_diff.append(i)
                dcm_list_diff.append(j)

        # Write results into CSV file
        with open(self.diff_output_file+".csv", 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['MERGE_LOADER_PREMIUMTRANS', 'DCM_LOADER_PREMIUMTRANS'])
            writer.writerows(zip_longest(*[merge_list_diff, dcm_list_diff]))

if __name__ == '__main__':
    ARG_PARCER = argparse.ArgumentParser(description='Loading')
    ARG_PARCER.add_argument('--conf-dir', required=True, dest='conf_dir', metavar='', help='Path to config folder')
    ARG_PARCER.add_argument('--conf-file', required=False, dest='conf_file', metavar='', help='Path to config folder')
    ARGS = ARG_PARCER.parse_args()

     # read arguments pass arguments from configuration
    CONF_DIR = ARGS.conf_dir
    CONF_FILE = ARGS.conf_file
    # load configuration file
    CONF_FILE_NAME = join(CONF_DIR, CONF_FILE)
    CONFIG = configparser.ConfigParser()
    CONFIG.optionxform = str
    CONFIG.read(CONF_FILE_NAME)
    Compare = Comparison(CONFIG)
    Compare.files_comparison_v2()
