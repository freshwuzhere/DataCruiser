import yaml as yl

def open_yaml(file_name):
    stream = open(file_name, 'r')
    all_str = yl.load(stream)
    stream.close()
    return all_str


def write_yaml(file_name, data_dic):
    # data_dic = {'today_str':'2016-11-26',}
    stream = open(file_name, 'w')
    yl.dump(data_dic, stream)    # Write a YAML representation of data to 'document.yaml'
    stream.close()
