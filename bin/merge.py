"""
OPAS to DCM Extractor.
Created On: 2/3/2021
Created by : e45003
"""


from state_utils import Utilities


class MergeFiles(Utilities):
    """MergeFiles class"""

    def __init__(self, resource_manager, config):
        """Init class"""
        super().__init__(resource_manager, config)

        self.output = config['file']['output']
        self.alip = config['file']['aliptodcm_premtrans']
        self.opas = config['file']['opas_premtrans']
        self.as400 = config['file']['as400_premtrans']

    def merge(self):
        """ Main merge function"""
        output = open(self.output + 'MERGE_LOADER_PREMIUMTRANS.txt', 'w', newline='')

        header = "|".join(self.target_columns)

        output.write(header + "\n")
        count = 0

        with open(self.alip) as alip:
            next(alip)
            for line in alip:
                output.write(line)
                count += 1

        with open(self.opas) as opas:
            next(opas)
            for line in opas:
                output.write(line)
                count += 1

        with open(self.as400) as as400:
            next(as400)
            for line in as400:
                output.write(line)
                count += 1

        output.write("Footer")


def load_merge(resource, config):
    """ Function to load merge class"""
    obj = MergeFiles(resource, config)
    obj.merge()
