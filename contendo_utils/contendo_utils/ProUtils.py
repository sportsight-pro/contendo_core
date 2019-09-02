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

#pu = ProUtils()
#print(pu.commastring_to_liststring('a,b,c,d'))
