import json
import os
from pathlib import Path

class ProUtils:
    def __init__(self):
        return

    @staticmethod
    def pandas_df_to_dict(in_df, key):
        ret_dict = {}
        for i, config in in_df.iterrows():
            ret_dict[config[key]] = dict(config)

        return ret_dict

    @staticmethod
    def format_string(string, instructions):
        if type(string)!=str:
            return ''
        outstring = string
        for key,value in instructions.items():
            outstring=outstring.replace('{'+key+'}', str(value))

        return outstring

    #
    # utility to add a record to a dict object
    @staticmethod
    def update_dict(inDict, updateDict, keys, suffix=''):
        for key in keys:
            if key in updateDict.keys():
                inDict['{}{}'.format(key, suffix)] = updateDict[key]
        return inDict

    #
    # utility to add a record to a dict object
    @staticmethod
    def commastring_to_liststring(commaString):
        ret = str(str.split(commaString, ',')).replace('[', '').replace(']', '')
        return ret


    @staticmethod
    def get_string_from_file(fileName):
        file = open(fileName, 'r')
        ret = file.read()
        file.close()
        return ret

    @staticmethod
    def write_string_to_file(fileName, text):
        file = open(fileName, 'w')
        file.write(text)
        file.close()
        return

    @staticmethod
    def get_dict_from_jsonfile(jsonFileName):
        jsonFile = open(jsonFileName, 'r')
        ret = json.load(jsonFile)
        jsonFile.close()
        return ret

    @staticmethod
    def save_dict_to_jsonfile(jsonFileName, dict):
        jsonFile = open(jsonFileName, 'w')
        json.dump(dict, jsonFile)
        jsonFile.close()
        return

    @staticmethod
    def create_path_directories(path):
        while not os.path.exists(Path(path).parent):
            ProUtils.create_path_directories(Path(path).parent)
            os.mkdir(Path(path).parent)

#pu = ProUtils()
#print(pu.commastring_to_liststring('a,b,c,d'))
