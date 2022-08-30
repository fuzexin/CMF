import debugpy
# Allow other computers to attach to debugpy at this IP address and port.
debugpy.listen(('172.20.201.90', 5678))
# Pause the program until a remote debugger is attached
debugpy.wait_for_client()


import pandas as pd
import logging, pickle, os
import numpy as np
import sent2vec
import datetime,dgl,torch
from dgl.data.utils import save_graphs
import getTextToken

def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn

logging.basicConfig(level=logging.INFO)
# ------------------------utils-----------------------------------------------------------
# ----------------------------------------------------------------------------------------

def gen_graph(data,edge_indices, depos_dir):
    # load the entity and evencode dict
    entity_dict = pd.read_csv(os.path.join(depos_dir, "loc_entity2id.txt"), \
        names=['id','entity_name'], sep='\t')
    entity_dict = dict(zip(list(entity_dict['entity_name']),list(entity_dict['id'])))
    code_dict = pd.read_csv("/data/fuzexin/Program/CMF/code/data/cameo.txt",sep='\t',\
        names=['code'],dtype=str,index_col=[1])
    logging.debug( range(len(code_dict['code'])))
    code_dict = dict(zip(list(code_dict['code']), range(len(code_dict['code']))))

    # get index of actor1, eventcode, actor2
    index_data = []
    for i in range(len(data)):
        actor1 = entity_dict[data[i][1]]
        eventcode = code_dict[data[i][2]]
        actor2 = entity_dict[data[i][3]]
        index_data.append([actor1,eventcode,actor2])
    
    # use the index data to generate graph
    index_data = np.array(index_data)
    edge_indices = np.array(edge_indices)
    g = get_big_graph_w_idx(index_data, edge_indices)

    return g


def get_big_graph_w_idx(data, edge_indices):
    # from  GLEAN's 1_get_digraphs.py file
    # data:the data is one date's data, which composed as (subject, relation, object)
    # edge_indices: the every piece of 'data' in the total data's position(index).
    
    # separate the different composition of data
    src, rel, dst = data.transpose()
    # get unique entity, including subject and object, 
    # edges is the src and dst's position in the unique list. 
    # np.unique(***,return_inverse=True) :return the original data's index 
    # in the generated unique list if the return_inverse = True.
    uniq_v, edges = np.unique((src, dst), return_inverse=True)
    # now the 'src' and 'dst' is the index in the unique list.
    src, dst = np.reshape(edges, (2, -1))
    # use dgl for handling graph
    g = dgl.DGLGraph()
    g.add_nodes(len(uniq_v))
    g.add_edges(src, dst, {'eid': torch.from_numpy(edge_indices)}) # array list
    norm = comp_deg_norm(g)
    g.ndata.update({'id': torch.from_numpy(uniq_v).long().view(-1, 1), 'norm': norm.view(-1, 1)})
    g.edata['r'] = torch.LongTensor(rel)
    g.ids = {}
    idx = 0
    for id in uniq_v:
        g.ids[id] = idx
        idx += 1
    return g


def comp_deg_norm(g):
    in_deg = g.in_degrees(range(g.number_of_nodes())).float()
    in_deg[torch.nonzero(in_deg == 0, as_tuple=False).view(-1)] = 1
    norm = 1.0 / in_deg
    return norm


# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------

# generate loc2id.txt
def loc2id(loc_list, depos_path):
    loc_data = pd.DataFrame(loc_list)
    loc_data.to_csv(depos_path,sep='\t',header=False)
    logging.info("loc2id.txt OK!")

def loc_entity2id(data, depos_path):
    entity = set()
    for loc_data in data:
        for one_data in loc_data:
            if len(one_data[1])>0:
                entity.add(one_data[1])
            if len(one_data[3])>0:
                entity.add(one_data[3])
    entity = list(entity)
    entity.sort()
    entity = pd.DataFrame(zip(range(len(entity)), entity))
    entity.to_csv(depos_path,sep='\t',index=False,header=False,doublequote=False)
    logging.info("loc_entity2id.txt OK!")

def data_count(data, depos_path):
    # get the count for every 292 event type for one day
    # we assume the data is permutating in the date order
    # get cameo data
    cameo_data = pd.read_csv("/data/fuzexin/Program/CMF/code/data/cameo.txt",sep='\t',\
        names=['code', 'description'],dtype=str)
    logging.debug( range(len(cameo_data['code'])))
    cameo_dict = dict(zip(list(cameo_data['code']), range(len(cameo_data['code']))))
    
    # stat the every day's event type number
    count_data = []
    for loc_data in data:
        day_data = []
        date_flag = loc_data[0][-1]
        i= 0 
        while i < len(loc_data) + 1:
            if i < len(loc_data) and loc_data[i][-1] == date_flag:
                day_data.append(loc_data[i])
                i += 1
            else:
                day_count = np.zeros(len(cameo_data['code']), dtype= float)
                for one_data in day_data:
                    day_count[cameo_dict[one_data[2]]] += 1
                count_data.append(day_count)
                
                day_data.clear()
                if i != len(loc_data):
                    date_flag = loc_data[i][-1]
                else:
                    i += 1
    
    count_data = np.array(count_data)
    
    # save the data
    with open(depos_path,'wb') as f:
        pickle.dump(count_data, f)
    
    logging.info("data_count.pkl OK!")


def text_emb(data, depos_dir, model_path):
    # aquire allnames as text token
    text_token_all = []
    for local_data in data:
        for one_data in local_data:
            text_token_all.append(one_data[4])
    
    # remove duplicated data
    # text_token, indices = np.unique(text_token,return_inverse=True)
    text_token = list(set(text_token_all))
    text_token = dict(zip(text_token, range(len(text_token))))
    indices = [text_token[one_text] for one_text in text_token_all]
    
    temp_text = list(range(len(text_token)))
    for one_text in text_token:
        temp_text[text_token[one_text]] = one_text
    
    text_token = temp_text
    del(temp_text)

    # remove stop words and get stem words
    text_token = getTextToken.process_texts(text_token)
    for i in range(len(text_token)):
        text_token[i] = ' '.join(text_token[i])

    # get embedding, model is trained in GLEAN process
    model = sent2vec.Sent2vecModel()
    model.load_model(model_path)
    embs = model.embed_sentences(text_token)
    # check
    logging.debug(embs)
    # storage data
    with open(os.path.join(depos_dir, "loc_text_emb.pkl"),'wb') as f:
        pickle.dump(embs, f)
    logging.info("loc_text_emb.pkl OK!")
    
    with open (os.path.join(depos_dir, "emb_idx.pkl"),'wb') as f:
        pickle.dump(indices, f)
    logging.info("emb_idx.pkl OK!")
    


def data_label(data, depos_dir):

    # load text_token embedding index data
    with open (os.path.join(depos_dir, "emb_idx.pkl"),'rb') as f:
        indices = pickle.load(f)
    # traverse the data, handle once for one day
    # initialize data
    time_data = []
    loc_data = []
    label_dup = []
    label= []
    text_id = []

    day_data = []
    first_date = datetime.datetime.strptime(data[0][0][-1], '%Y-%m-%d')

    # record all other location data before this location data, 
    # because data including more than 1 position's data
    pos_num_flag = 0
    # record one day's text embedding id, order required.
    day_text_id = []
    # i record loc, j record every piece of data
    i = 0
    for i in range(len(data)):
        date_flag = data[i][0][-1]
        j = 0
        if i > 0:
            pos_num_flag += len(data[i-1])
        while j < len(data[i])+1:
            if j < len(data[i]) and data[i][j][-1] == date_flag:
                day_data.append(data[i][j])
                # record oneday's text_id
                day_text_id.append(indices[pos_num_flag+j])
                j += 1
            else:
                # get date
                the_date = datetime.datetime.strptime(date_flag, '%Y-%m-%d')
                the_date = (the_date - first_date).days
                time_data.append(the_date)

                # get loc_data
                loc_data.append(i)

                # get label_dup
                day_dup = {}
                for one_data in day_data:
                    # label
                    root_code = int(one_data[2][0:2])-1
                    day_dup[root_code] = day_dup.setdefault(root_code,0) + 1

                label_dup.append(day_dup)
                
                # get the label data
                label.append(list(day_dup.keys()))
                
                # get the text_id data
                text_id.append(np.array(day_text_id))
                logging.info(f"loc: {i}, date: {date_flag} data has get")
                # prepare for next day  
                day_data.clear()
                day_text_id.clear()
                if j != len(data[i]):
                    date_flag = data[i][j][-1]
                else:
                    j += 1

    data_label = {'time':time_data, 'loc':loc_data, 'label':label,'label_dup':label_dup, 'text_id':text_id}
    with open(os.path.join(depos_dir, "data_label.pkl"), 'wb') as f:
        pickle.dump(data_label, f)
    logging.info("data_label.pkl OK!")
    

def data_graph(data, depos_dir):

    # graph list for storaging graph data
    graph_list = []
    for loc_data in data:
        # record for the first index of the same date data
        first_index = 0
        for i in range(len(loc_data)):
            if loc_data[i][-1] != loc_data[first_index][-1]:
                # data[i][-1] means the i-th data's EventDate
                
                # generate a graph for the same date data
                g1 = gen_graph(loc_data[first_index:i], list(range(first_index,i)), depos_dir)

                # arrange to the graph list
                graph_list.append(g1)

                # set for next date
                first_index = i
        
        # in the end, we shall process the tail data specially
        g1 = gen_graph(loc_data[first_index:len(loc_data)], list(range(first_index, len(loc_data))), depos_dir)
        graph_list.append(g1)

    # check the graph list data
    logging.debug(graph_list)

    # save the graph data
    save_graphs(os.path.join(deposit_dir, 'data_graph.bin'), graph_list)
    logging.info('data_graph.bin OK')
    




if __name__ == "__main__":
    
    # parameters setting
    # there are many cities in one country in implementation of CMF
    loc_list = ["Abuja", "Alexandria", 'Buhari',"Cairo", "Lagos"]
    # where to deposit all generated data file
    deposit_dir = "/data/fuzexin/Program/CMF/code/data/EG2"
    # GDELT data dir
    gdelt_dir = r"/data/fuzexin/Program/CMF/code/data/EG2/GDELTData"
    # sen2vec model path
    model_path = r'/data/fuzexin/Program/CMF/code/data/THAI/s2v_300.bin'

    """ 1, get loc2id.txt """
    # loc2id(loc_list, os.path.join(deposit_dir, 'loc2id.txt'))
    
    # get the GDELT data
    data = []
    for one_loc in loc_list:
        one_path = os.path.join(gdelt_dir, one_loc+'.pkl')
        with open (one_path,'rb') as f:
            data.append(pickle.load(f))
    
    """ 2, get loc_entity2id.txt """
    # loc_entity2id(data, os.path.join(deposit_dir, 'loc_entity2id.txt'))
    
    """ 3, get data_count.pkl """
    # data_count(data, os.path.join(deposit_dir, 'data_count.pkl'))

    """ 4, get loc_text_emb.pkl """
    # text_emb(data, deposit_dir, model_path)
    
    """ 5, get data_label.pkl """
    data_label(data, deposit_dir)

    """ 5, data_graph.bin """
    data_graph(data, deposit_dir)

    