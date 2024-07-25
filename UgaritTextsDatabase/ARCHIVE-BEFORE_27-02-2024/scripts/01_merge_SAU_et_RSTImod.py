""" This script is an application of the join_SAU_UTN.ipynb """

import pandas as pd
import numpy as np
import os
import re

ROOT_PATH = os.getcwd()
INPUT_DBN_PATH = os.path.join(ROOT_PATH, 'RSTI_MODIFIED.csv')
INPUT_SAU_PATH = os.path.join(ROOT_PATH, 'SAU_COLLECTION.csv')
OUTPUT_PATH = os.path.join(ROOT_PATH, 'UGARIT_TEXTS_DATABASE.csv')


empty_entry_dict = {
        'Excavation Siglum': np.nan,
        'Excavation Numbers "Cleared"': np.nan,
        'Excavation Numbers UTDB (+-RSTI)': np.nan,
        'Excavation Numbers SAU': np.nan,
        'KTU3': np.nan,
        'SAU Biblio (not full!!)': np.nan,
        'Script': np.nan,
        'Language': np.nan,
        'SAU Script': np.nan,
        'SAU Language': np.nan,
        'SAU Script et Language': np.nan,
        'SAU genre': np.nan,
        'RSTI object type': np.nan,
        'RSTI description': np.nan,
        'Archive/General area': np.nan,
        'SAU Archive/General area': np.nan,
        'Detail in General': np.nan,
        'SAU Detail in General': np.nan,
        'SAU findspot': np.nan,
        'possible relevant (religious) genres': np.nan,
        '"Clemens 2001"': np.nan,
        'Clemens Type': np.nan,
        'Clemens Note': np.nan,
        'Seal': np.nan,
        'Size': np.nan,
        'Museal Siglum': np.nan,
        'SAU Museal Siglum': np.nan,
        'TEO findspot': np.nan,
        'Topographic Point': np.nan,
        'SAU p.t.': np.nan,
        'Depth': np.nan,
        'SAU Depth': np.nan,
        'Note': np.nan
    }



def interpret_SAU_desses(full_RS:str)->list:
    text = full_RS.replace('(+)', ' ').replace('[+]', ' ').replace('+', ' ').replace('(=)', ' ').replace('[=]', ' ').replace('=', ' ').replace('|', ' ')

    pattern = r'\[[^\]]*\]|\S+'
    result = re.findall(pattern, text)
    
    return result


def clear_rs_num(orig_rs_num:str)->str:
    cleared_rs = ''
    for char in orig_rs_num:
        if char.isnumeric() or char.isalpha() or char =='.' or char == ',' or char == '-':
            cleared_rs += char
    
    return cleared_rs


def compare_entries(dict_1:dict, dict_2:dict):
    """ This function compares to dicts that consist of keys pointing to lists. """
    # Vytvoření slovníku pro ukládání výsledků
    result = {}

    # Získání všech unikátních hodnot z obou slovníků
    all_values = set(val for values in dict_1.values() for val in values) | set(val for values in dict_2.values() for val in values)

    # Pro každou hodnotu zjistíme, v kterých klíčích se nachází v obou slovnících
    for val in all_values:
        key1 = next((k for k, v in dict_1.items() if val in v), None)
        key2 = next((k for k, v in dict_2.items() if val in v), None)
        result[val] = (key1, key2)

    return result


def clear_all_entries(full_entries:list):
    rs_nums_on_line_cleared = []
    for rs in full_entries:
        rs_clear = clear_rs_num(rs)
        rs_nums_on_line_cleared.append(rs_clear)
        
    rs_nums_on_line_cleared.sort()
        
    rs_nums_on_line_cleared = list(set(rs_nums_on_line_cleared))
    
    return rs_nums_on_line_cleared


def detect_SAU_script_et_language(SAU_scr_lang:str):
    if pd.isna(SAU_scr_lang):
        return np.nan, np.nan, np.nan
    else:    
        SAU_scr_lang = SAU_scr_lang.lower()
        parts_of_design = SAU_scr_lang.split('|')

        all_parts = []
        for pod in parts_of_design:
            if pod == 's-cy' or pod == 's-cy?':
                all_parts.append(pod)
            else:
                more_parts = pod.split('-')
                for mp in more_parts:
                    even_more_parts = mp.split(';')
                    all_parts.extend(even_more_parts)
            
        lang_designs = {'su': 'Sumerian', 'su?': 'Sumerian?', '[su]?': '[Sumerian?]', '[su?]': '[Sumerian?]', '[su]': '[Sumerian]', 'hu': 'Hurrian', 'hu?': 'Hurrian?', 'ak': 'Akkadian', '[ak]': '[Akkadian]', 'ak?': 'Akkadian?', '(ak)': '(Akkadian)', '[ak?]': '[Akkadian?]', 'ug': 'Ugaritic','[ug]': '[Ugaritic]', 'ug (mirror)': 'Ugaritic', 'ug?': 'Ugaritic?', 'hi': 'Hittite', 'hi?': 'Hittite?', 'eg': 'Egyptian', 'eg?': 'Egyptian?', 'p': 'Phoenician'}
        script_designs = {'a': 'Alphabetic', '(a)': '(Alphabetic)', 's': 'Logosyllabic', 'h': 'Hieroglyphs', 'a/s': 'Alphabetic/Logosyllabic', 's/a': 'Alphabetic/Logosyllabic'}
        
        SAU_languages = ''
        SAU_scripts = ''
        
        for part in all_parts:
            if part == 's-cy':
                SAU_languages += f';Cypro-Minoan'
            elif part == 's-cy?':
                SAU_languages += f';Cypro-Minoan?'
            elif part in lang_designs:
                SAU_languages += f';{lang_designs[part]}'
            elif part in script_designs:
                SAU_scripts += f';{script_designs[part]}'
            else:
                # if part == '':
                #     continue
                # else:
                #     print('\t'+part)
                continue
                
        SAU_languages = SAU_languages[1:]
        SAU_scripts = SAU_scripts[1:]
        
        return SAU_scr_lang, SAU_languages, SAU_scripts
    
    
def extract_desired_info_from_DBN_entry(DBN_line_index, DBN_dataframe):
    line_data_dict = DBN_dataframe.loc[DBN_line_index].to_dict()
    
    transformed_dictionary = {
        'Excavation Siglum': line_data_dict['Excavation Siglum'],
        'Excavation Numbers "Cleared"': 'np.nan',
        'Excavation Numbers UTDB (+-RSTI)': line_data_dict['Excavation Numbers'],
        'Excavation Numbers SAU': np.nan,
        'KTU3': line_data_dict['KTU3'],
        'SAU Biblio (not full!!)': np.nan,
        'Script': line_data_dict['Script'],
        'Language': line_data_dict['Language'],
        'SAU Script': np.nan,
        'SAU Language': np.nan,
        'SAU Script et Language': np.nan,
        'SAU genre': np.nan,
        'RSTI object type': line_data_dict['Object Type'],
        'RSTI description': line_data_dict['Description'],
        'Archive/General area': line_data_dict['Archive/General area'],
        'SAU Archive/General area': np.nan,
        'Detail in General': line_data_dict['detail in general'],
        'SAU Detail in General': np.nan,
        'SAU findspot': np.nan,
        'possible relevant (religious) genres': line_data_dict['possible relevant (religious) genres'],
        '"Clemens 2001"': line_data_dict['Clemens 2001'],
        'Clemens Type': line_data_dict['Clemens 2001 type'],
        'Clemens Note': line_data_dict['Clemens note'],
        'Seal': line_data_dict['seal'],
        'Size': line_data_dict['Size'],
        'Museal Siglum': line_data_dict['Museum Number'],
        'SAU Museal Siglum': np.nan,
        'TEO findspot': line_data_dict['Full TEO Findspot'],
        'Topographic Point': line_data_dict['Topographic Point'],
        'SAU p.t.': np.nan,
        'Depth': line_data_dict['Find Depth'],
        'SAU Depth': np.nan,
        'Note': line_data_dict['note']
    }
    
    return transformed_dictionary


def extract_desired_info_from_SAU_entry(SAU_line_index=None, SAU_dataframe=None, already_line_dictionary=None):
    if already_line_dictionary:
        line_data_dict = already_line_dictionary
    else:
        line_data_dict = SAU_dataframe.loc[SAU_line_index].to_dict()
        
    SAU_scr_lang, SAU_languages, SAU_scripts = detect_SAU_script_et_language(line_data_dict['script'])
    
    transformed_dictionary = {
        'Excavation Siglum': 'RS',
        'Excavation Numbers "Cleared"': 'np.nan',
        'Excavation Numbers UTDB (+-RSTI)': np.nan,
        'Excavation Numbers SAU': line_data_dict['RS'],
        'KTU3': np.nan,
        'SAU Biblio (not full!!)': line_data_dict['bibliography'],
        'Script': np.nan,
        'Language': np.nan,
        'SAU Script': SAU_scripts,
        'SAU Language': SAU_languages,
        'SAU Script et Language': SAU_scr_lang,
        'SAU genre': line_data_dict['genre'],
        'RSTI object type': np.nan,
        'RSTI description': np.nan,
        'Archive/General area': np.nan,
        'SAU Archive/General area': line_data_dict['archive/general area'],
        'Detail in General': np.nan,
        'SAU Detail in General': line_data_dict['detail in general'],
        'SAU findspot': line_data_dict['loc'],
        'possible relevant (religious) genres': np.nan,
        '"Clemens 2001"': np.nan,
        'Clemens Type': np.nan,
        'Clemens Note': np.nan,
        'Seal': np.nan,
        'Size': np.nan,
        'Museal Siglum': np.nan,
        'SAU Museal Siglum': line_data_dict['museal sig'],
        'TEO findspot': np.nan,
        'Topographic Point': np.nan,
        'SAU p.t.': line_data_dict['pt'],
        'Depth': np.nan,
        'SAU Depth': line_data_dict['depth'],
        'Note': np.nan
    }
    
    return transformed_dictionary
    

def merge_entries(dict_DBN:dict, dict_SAU:dict, keep_ex_num:str)->dict:    
    out_dict = {}
    for key in dict_DBN:
        if key == 'Excavation Numbers "Cleared"':
            try:
                entries_in_DBN = eval(dict_DBN['Excavation Numbers UTDB (+-RSTI)'])
                cleared_entries_in_DBN = clear_all_entries(entries_in_DBN)
            except:
                cleared_entries_in_DBN = []
                
            try:
                entries_in_SAU = interpret_SAU_desses(dict_SAU['Excavation Numbers SAU'])
                cleared_entries_in_SAU = clear_all_entries(entries_in_SAU)
            except:
                cleared_entries_in_SAU = []
            
            if keep_ex_num == 'DBN':
                cleared_entries_in_DBN.sort()
                out_dict['Excavation Numbers "Cleared"'] = cleared_entries_in_DBN
            elif keep_ex_num == 'SAU':
                cleared_entries_in_SAU.sort()
                out_dict['Excavation Numbers "Cleared"'] = cleared_entries_in_SAU
            elif keep_ex_num == 'both':
                all_entries = []
                for entry in cleared_entries_in_SAU:
                    all_entries.append(entry)
                for entry in cleared_entries_in_DBN:
                    all_entries.append(entry)
                all_entries.sort()
                out_dict['Excavation Numbers "Cleared"'] = list(set(all_entries))
            else:
                print('EORROR in Clearing numbers!!!', cleared_entries_in_DBN, cleared_entries_in_SAU)
        elif pd.isna(dict_DBN[key]) and pd.isna(dict_SAU[key]):
            out_dict[key] = np.nan
        elif dict_DBN[key] != np.nan and pd.isna(dict_SAU[key]):
            out_dict[key] = dict_DBN[key]
        elif pd.isna(dict_DBN[key]) and dict_SAU[key] != np.nan:
            out_dict[key] = dict_SAU[key]
        else:
            if dict_DBN[key] == dict_SAU[key]:
                out_dict[key] = dict_DBN[key]
            else:
                out_dict[key] = f'{dict_DBN[key]}||{dict_SAU[key]}'
                
    return out_dict


def merge_dicts(list_of_dicts:list)->dict:
    merged_dict = {}

    for d in list_of_dicts:
        for key, value in d.items():
            if key in merged_dict:
                if merged_dict[key] != value and (not pd.isna(merged_dict[key]) or not pd.isna(value)):
                    merged_dict[key] = f"{merged_dict[key]}|{value}"
            else:
                merged_dict[key] = value
    
    return merged_dict

def remove_duplicates_in_dict(input_dict:dict)->dict:
    for key, value in input_dict.items():
        try:
            input_dict[key] = '|'.join([x for x in set(value.split('|'))])
        except AttributeError:
            input_dict[key] = np.nan
        
        if input_dict[key] == 'nan':
            input_dict[key] = np.nan
        elif type(input_dict[key]) == str:
            input_dict[key] = input_dict[key].replace('|nan', '')
            input_dict[key] = input_dict[key].replace('nan|', '')
            input_dict[key] = input_dict[key].replace('nan', '')
        else:
            continue
    
    return input_dict

def merge_multiple_SAU_entries_to_one_dict(SAU_dataframe, SAU_indexes:list)->dict:
    all_relevant_dicts = []   
    for idx in SAU_indexes:
        if idx == None:
            continue
        else:
            line_data = SAU_dataframe.loc[idx].to_dict()
            all_relevant_dicts.append(line_data)
        
    merged_dict = merge_dicts(all_relevant_dicts)
    
    merged_dict = remove_duplicates_in_dict(merged_dict)
    
    return merged_dict


def main():
    """ Preparation step 1 - checking RS nums in SAU and DBN """
    SAU_df = pd.read_csv(INPUT_SAU_PATH, encoding='utf-8', delimiter=';', dtype=str)

    RS_nums_in_SAU = []

    for idx in SAU_df.index:
        rs_cleared_on_line = []
        rs_nums_on_line = interpret_SAU_desses(SAU_df.loc[idx]['RS'])
        for rs in rs_nums_on_line:
            rs_clear = clear_rs_num(rs)
            if rs_clear in RS_nums_in_SAU:
                    print(rs_clear, rs, 'already in another entry!!')
            if rs_clear in rs_cleared_on_line:
                    print(rs_clear, rs, '\talready on line')
            rs_cleared_on_line.append(rs_clear)
        RS_nums_in_SAU.extend(rs_cleared_on_line)
        
    print('Number of excavation sigla in SAU', len(RS_nums_in_SAU))


    DBN_df = pd.read_csv(INPUT_DBN_PATH, encoding='utf-8', delimiter=';', dtype=str)

    RS_nums_in_DBN = []

    for idx in DBN_df.index:
        line_data = DBN_df.loc[idx].to_dict()

        rs_cleared_on_line = []
        
        rs_nums_on_line = eval(line_data['Excavation Numbers'])
        for rs in rs_nums_on_line:
            rs_clear = clear_rs_num(rs)
            if rs_clear in RS_nums_in_DBN:
                    print(rs_clear, rs, 'already in another entry!!')
            if rs_clear in rs_cleared_on_line:
                    print(rs_clear, rs, '\talready on line')
            rs_cleared_on_line.append(rs_clear)
        
        RS_nums_in_DBN.extend(rs_cleared_on_line)
        
    print('Number of excavation sigla in DBN', len(RS_nums_in_DBN))
    print()
    
    """ Comparing and clearing the entries. """
    
    SAU_df = pd.read_csv(INPUT_SAU_PATH, encoding='utf-8', delimiter=';', dtype=str)
    DBN_df = pd.read_csv(INPUT_DBN_PATH, encoding='utf-8', delimiter=';', dtype=str)

    entries_in_SAU = {}

    for idx in SAU_df.index:
        rs_nums_on_line_cleared = []
        rs_nums_on_line = interpret_SAU_desses(SAU_df.loc[idx]['RS'])
        for rs in rs_nums_on_line:
            rs_clear = clear_rs_num(rs)
            rs_nums_on_line_cleared.append(rs_clear)
            
        entries_in_SAU[idx] = rs_nums_on_line_cleared
        
    print('Entries in SAU', len(entries_in_SAU))

    entries_in_DBN = {}

    for idx in DBN_df.index:
        line_data = DBN_df.loc[idx].to_dict()
        rs_nums_on_line_cleared = []
        rs_nums_on_line = eval(line_data['Excavation Numbers'])
        for rs in rs_nums_on_line:
            rs_clear = clear_rs_num(rs)
            rs_nums_on_line_cleared.append(rs_clear)
            
        entries_in_DBN[idx] = rs_nums_on_line_cleared
        
    print('Entries in DBN', len(entries_in_DBN))
    print()

    compared_entries_in_SAU_et_DBN = compare_entries(entries_in_SAU, entries_in_DBN)
    
    """ Preparing the data from the perspective of SAU """
    
    SAU_df = pd.read_csv(INPUT_SAU_PATH, encoding='utf-8', delimiter=';', dtype=str)
    DBN_df = pd.read_csv(INPUT_DBN_PATH, encoding='utf-8', delimiter=';', dtype=str)

    truly_fully_overlapped_entries_SAU = {}
    fully_overlapped_entries_SAU = {}
    partially_overlapped_entries_with_None_SAU = {}
    entries_in_SAU_but_not_in_DBN = []
    idxs_in_SAU_associated_with_more_lines_in_DBN = {}

    for idx_SAU in SAU_df.index:
        rs_nums_of_entry_in_SAU = interpret_SAU_desses(SAU_df.loc[idx_SAU]['RS'])
        cleared_rs_nums_in_SAU_entry = clear_all_entries(rs_nums_of_entry_in_SAU)
        
        associated_indexes_in_DBN_for_this_SAU_entry = []
        for SAU_rs in cleared_rs_nums_in_SAU_entry:
            DBN_idx = compared_entries_in_SAU_et_DBN[SAU_rs][1]
            associated_indexes_in_DBN_for_this_SAU_entry.append(DBN_idx)
            
        associated_indexes_in_DBN_for_this_SAU_entry = list(set(associated_indexes_in_DBN_for_this_SAU_entry))
        
        if len(associated_indexes_in_DBN_for_this_SAU_entry) == 1 and associated_indexes_in_DBN_for_this_SAU_entry != [None]:
            cleared_entries_in_DBN_for_this_idx = clear_all_entries(eval(DBN_df.loc[associated_indexes_in_DBN_for_this_SAU_entry[0]]['Excavation Numbers']))
            if cleared_rs_nums_in_SAU_entry == cleared_entries_in_DBN_for_this_idx:
                truly_fully_overlapped_entries_SAU[idx_SAU] = associated_indexes_in_DBN_for_this_SAU_entry[0]
        
        if associated_indexes_in_DBN_for_this_SAU_entry == [None]:
            entries_in_SAU_but_not_in_DBN.append(idx_SAU)
        elif len(associated_indexes_in_DBN_for_this_SAU_entry) == 1 and idx_SAU not in truly_fully_overlapped_entries_SAU:
            fully_overlapped_entries_SAU[idx_SAU] = associated_indexes_in_DBN_for_this_SAU_entry[0]
        elif len(associated_indexes_in_DBN_for_this_SAU_entry) == 2 and None in associated_indexes_in_DBN_for_this_SAU_entry:
            for DIDX in associated_indexes_in_DBN_for_this_SAU_entry:
                if DIDX == None:
                    continue
                else:
                    the_DBN_idx_for_this_entry = DIDX
            partially_overlapped_entries_with_None_SAU[idx_SAU] = the_DBN_idx_for_this_entry
        elif len(associated_indexes_in_DBN_for_this_SAU_entry) >= 2:
            idxs_in_SAU_associated_with_more_lines_in_DBN[idx_SAU] = associated_indexes_in_DBN_for_this_SAU_entry
            

    print('truly_fully_overlapped_entries_SAU', len(truly_fully_overlapped_entries_SAU))
    print('fully_overlapped_entries_SAU', len(fully_overlapped_entries_SAU))
    print('partially_overlapped_entries_with_None_SAU', len(partially_overlapped_entries_with_None_SAU))
    print('entries_in_SAU_but_not_in_DBN', len(entries_in_SAU_but_not_in_DBN))
    print('idxs_in_SAU_associated_with_more_lines_in_DBN', len(idxs_in_SAU_associated_with_more_lines_in_DBN))
    print()
    
    """ Preparing the data from the perspective of DBN """
    
    SAU_df = pd.read_csv(INPUT_SAU_PATH, encoding='utf-8', delimiter=';', dtype=str)
    DBN_df = pd.read_csv(INPUT_DBN_PATH, encoding='utf-8', delimiter=';', dtype=str)

    truly_fully_overlapped_entries_DBN = {}
    fully_overlapped_entries_DBN = {}
    partially_overlapped_entries_with_None_DBN = {}
    entries_in_DBN_but_not_in_SAU = []
    idxs_in_DBN_associated_with_more_lines_in_SAU = {}

    for idx_DBN in DBN_df.index:
        rs_nums_of_entry_in_DBN = eval(DBN_df.loc[idx_DBN]['Excavation Numbers'])
        cleared_rs_nums_in_DBN_entry = clear_all_entries(rs_nums_of_entry_in_DBN)

        associated_indexes_in_SAU_for_this_DBN_entry = []
        for DBN_rs in cleared_rs_nums_in_DBN_entry:
            SAU_idx = compared_entries_in_SAU_et_DBN[DBN_rs][0]
            associated_indexes_in_SAU_for_this_DBN_entry.append(SAU_idx)
            
        associated_indexes_in_SAU_for_this_DBN_entry = list(set(associated_indexes_in_SAU_for_this_DBN_entry))
        
        if len(associated_indexes_in_SAU_for_this_DBN_entry) == 1 and associated_indexes_in_SAU_for_this_DBN_entry != [None]:
            cleared_entries_in_SAU_for_this_idx = clear_all_entries(interpret_SAU_desses(SAU_df.loc[associated_indexes_in_SAU_for_this_DBN_entry[0]]['RS']))
            if cleared_rs_nums_in_DBN_entry == cleared_entries_in_SAU_for_this_idx:
                truly_fully_overlapped_entries_DBN[idx_DBN] = associated_indexes_in_SAU_for_this_DBN_entry[0]        
        
        if associated_indexes_in_SAU_for_this_DBN_entry == [None]:
            entries_in_DBN_but_not_in_SAU.append(idx_DBN)
        elif len(associated_indexes_in_SAU_for_this_DBN_entry) == 1 and idx_DBN not in truly_fully_overlapped_entries_DBN:
            fully_overlapped_entries_DBN[idx_DBN] = associated_indexes_in_SAU_for_this_DBN_entry[0]
        elif len(associated_indexes_in_SAU_for_this_DBN_entry) == 2 and None in associated_indexes_in_SAU_for_this_DBN_entry:
            for SIDX in associated_indexes_in_SAU_for_this_DBN_entry:
                if SIDX == None:
                    continue
                else:
                    the_SAU_idx_for_this_entry = SIDX
            partially_overlapped_entries_with_None_DBN[idx_DBN] = the_SAU_idx_for_this_entry
        elif len(associated_indexes_in_SAU_for_this_DBN_entry) >= 2:
            idxs_in_DBN_associated_with_more_lines_in_SAU[idx_DBN] = associated_indexes_in_SAU_for_this_DBN_entry    
            
    print('truly_fully_overlapped_entries_DBN', len(truly_fully_overlapped_entries_DBN))
    print('fully_overlapped_entries_DBN', len(fully_overlapped_entries_DBN))
    print('partially_overlapped_entries_with_None_DBN', len(partially_overlapped_entries_with_None_DBN))
    print('entries_in_DBN_but_not_in_SAU', len(entries_in_DBN_but_not_in_SAU))
    print('idxs_in_DBN_associated_with_more_lines_in_SAU', len(idxs_in_DBN_associated_with_more_lines_in_SAU))
    print()
    
    """ Joining the dataframes: """
    """ First, create the output which is primarily based on DBN: """
    output_dictionary = {}

    out_idx = 0

    SAU_idxs_added_to_final_db = []

    for DBN_idx in DBN_df.index:
        # Option one: the entries in DBN and SAU reffer to the same excavation sigla --> merge entries, keep both ex sigla
        if DBN_idx in truly_fully_overlapped_entries_DBN:
            SAU_idx = truly_fully_overlapped_entries_DBN[DBN_idx]
            
            DBN_dict = extract_desired_info_from_DBN_entry(DBN_idx, DBN_df)
            SAU_dict = extract_desired_info_from_SAU_entry(SAU_idx, SAU_df)
            
            merged_entry = merge_entries(DBN_dict, SAU_dict, keep_ex_num='both')
            
            output_dictionary[out_idx] = merged_entry
            out_idx += 1
            
            SAU_idxs_added_to_final_db.append(SAU_idx)
                
        # Option two: the entry is only is DBN --> add DBN entry
        elif DBN_idx in entries_in_DBN_but_not_in_SAU:
            processed_line_dict = merge_entries(extract_desired_info_from_DBN_entry(DBN_idx, DBN_df), empty_entry_dict, keep_ex_num='DBN')
            output_dictionary[out_idx] = processed_line_dict
            out_idx += 1        
            
        # Option three: there is the "full overlap" (ie, there is more associated with this entry in SAU, but not in more entries; in DBN this is in more entries) --> keep the DBN entry for clear ex sigla
        elif DBN_idx in fully_overlapped_entries_DBN:
            SAU_idx = fully_overlapped_entries_DBN[DBN_idx]
            
            DBN_dict = extract_desired_info_from_DBN_entry(DBN_idx, DBN_df)
            SAU_dict = extract_desired_info_from_SAU_entry(SAU_idx, SAU_df)
            
            merged_entry = merge_entries(DBN_dict, SAU_dict, keep_ex_num='DBN')
            
            output_dictionary[out_idx] = merged_entry
            out_idx += 1
            
            SAU_idxs_added_to_final_db.append(SAU_idx)
        
        # Option four: There is partial overlap (some rs nums are not in SAU, but it is not associated with more entries) --> merge entries, keep both ex sigla
        elif DBN_idx in partially_overlapped_entries_with_None_DBN:
            SAU_idx = partially_overlapped_entries_with_None_DBN[DBN_idx]
            
            DBN_dict = extract_desired_info_from_DBN_entry(DBN_idx, DBN_df)
            SAU_dict = extract_desired_info_from_SAU_entry(SAU_idx, SAU_df)
            
            merged_entry = merge_entries(dict_DBN=DBN_dict, dict_SAU=SAU_dict, keep_ex_num='both')
            
            output_dictionary[out_idx] = merged_entry
            out_idx += 1        
            
            SAU_idxs_added_to_final_db.append(SAU_idx)
            
        # Option five: There are more entries in SAU associated with this text --> Add all info from the SAU entries to the final entry, but also save the SAU idxs to put them in the separate DF
        elif DBN_idx in idxs_in_DBN_associated_with_more_lines_in_SAU:
            DBN_dict = extract_desired_info_from_DBN_entry(DBN_idx, DBN_df)
            
            SAU_indexes = idxs_in_DBN_associated_with_more_lines_in_SAU[DBN_idx]
            
            SAU_dict = merge_multiple_SAU_entries_to_one_dict(SAU_dataframe=SAU_df, SAU_indexes=SAU_indexes)
            SAU_dict = extract_desired_info_from_SAU_entry(already_line_dictionary=SAU_dict)
            
            merged_entry = merge_entries(DBN_dict, SAU_dict, keep_ex_num='both')
            
            output_dictionary[out_idx] = merged_entry
            out_idx += 1        
            
            SAU_idxs_added_to_final_db.extend(SAU_indexes)
            
        else:
            print('There is some error with', DBN_idx)
            
    """ Then add more entries from the SAU dataframe. The output dictionary and its indexes continue from the previous cell """

    print('Size of the dataframe before adding SAU:', len(output_dictionary))

    for SAU_idx in SAU_df.index:
        # Option one: the SAU entry has already been processed:
        if SAU_idx in SAU_idxs_added_to_final_db:
            continue
        else:            
        # Option two: the entry is only is SAU --> add SAU entry
            if SAU_idx in entries_in_SAU_but_not_in_DBN:            
                processed_line_dict = merge_entries(extract_desired_info_from_SAU_entry(SAU_idx, SAU_df), empty_entry_dict, keep_ex_num='SAU')
                output_dictionary[out_idx] = processed_line_dict
                out_idx += 1
                
            # Option three: there is the "full overlap" (ie, there is more associated with this entry in DBN, but not in more entries; in SAU this is in more entries) --> keep the SAU entry for clear ex sigla ... this should have actually already been resolved, because we have went through DBN entries --> Check if this returns something
            elif SAU_idx in fully_overlapped_entries_SAU:
                print(f'This is weird - the DBN entry associated with SAU index{SAU_idx} should have already been processed! FULL OL')
                
            # Option four: There is partial overlap (some rs nums are not in DBN, but it is not associated with more entries) --> merge entries, keep both ex sigla ... this should have actually already been resolved, because we have went through DBN entries --> Check if this returns something
            elif SAU_idx in partially_overlapped_entries_with_None_SAU:
                print(f'This is weird - the DBN entry associated with SAU index{SAU_idx} should have already been processed! PARTIAL OL')
                
            # Option five: There are more entries in DBN associated with this text --> Add all info from the DBN entries to the final entry, but also save the DBN idxs to put them in the separate DF --> These line must have actually already been processed, too --> Check it!
            elif SAU_idx in idxs_in_SAU_associated_with_more_lines_in_DBN:
                print(f'This is weird - the DBN entry associated with SAU index{SAU_idx} should have already been processed! Multiple DBN lines!!')
                
            else:
                print('There is some error with', SAU_idx)
                            
    print('Size of the dataframe after adding SAU:', len(output_dictionary))
    print()
    
    """ Save the merged dict of both databases to CSV file """

    NEW_DF = pd.DataFrame.from_dict(output_dictionary, orient='index')
    NEW_DF.to_csv(OUTPUT_PATH, sep=';', encoding='utf-8')       
    
    """ Finally, export the SAU and DBN indexes that were in multiple entries to a separate databases, so they may be easily checked: """

    OUTPUT_PATH_MORE_ENTRIES_IN_SAU = os.path.join(ROOT_PATH, 'ENTRIES_IN_SAU_ASSOCIATED_WITH_MULTIPLE_UTDB_ENTRIES.csv')
    OUTPUT_PATH_MORE_ENTRIES_IN_DBN = os.path.join(ROOT_PATH, 'ENTRIES_IN_UTDB_ASSOCIATED_WITH_MULTIPLE_SAU_ENTRIES.csv')

    # NOTE: SAU
    SAU_multiple_dict = {}
    SAU_multiple_idx = 0
    for DBN_idx in idxs_in_DBN_associated_with_more_lines_in_SAU:
        dbn_rss = DBN_df.loc[DBN_idx]['Excavation Numbers']
        
        SAU_indexes = idxs_in_DBN_associated_with_more_lines_in_SAU[DBN_idx]
        for SAU_idx in SAU_indexes:
            if SAU_idx == None:
                continue
            else:
                line_data = SAU_df.loc[SAU_idx].to_dict()
                line_data['Associated UTDB(+-RSTI) Entry'] = dbn_rss
                SAU_multiple_dict[SAU_multiple_idx] = line_data
                SAU_multiple_idx += 1

    SAU_multiple_DF = pd.DataFrame.from_dict(SAU_multiple_dict, orient='index')
    SAU_multiple_DF.to_csv(OUTPUT_PATH_MORE_ENTRIES_IN_SAU, sep=';', encoding='utf-8')

    print(f'Separate CSV file for DBN entries with multiple SAU connections has been created. It contains {len(SAU_multiple_DF)} entries')

    # NOTE: DBN
    DBN_multiple_dict = {}
    DBN_multiple_idx = 0

    for SAU_idx in idxs_in_SAU_associated_with_more_lines_in_DBN:
        sau_rss = SAU_df.loc[SAU_idx]['RS']
        
        DBN_indexes = idxs_in_SAU_associated_with_more_lines_in_DBN[SAU_idx]

        for DBN_idx in DBN_indexes:
            if DBN_idx == None:
                continue
            else:
                line_data = DBN_df.loc[DBN_idx].to_dict()
                line_data['Associated SAU Entry'] = sau_rss
                DBN_multiple_dict[DBN_multiple_idx] = line_data
                DBN_multiple_idx += 1

    # Save the dict to CSV file
    DBN_multiple_DF = pd.DataFrame.from_dict(DBN_multiple_dict, orient='index')
    DBN_multiple_DF.to_csv(OUTPUT_PATH_MORE_ENTRIES_IN_DBN, sep=';', encoding='utf-8') 

    print(f'Separate CSV file for SAU entries with multiple DBN connections has been created. It contains {len(DBN_multiple_DF)} entires')
    
    print()
    
    print('The process in finished. Do not forget to run 02_backup-et-publish.py script to make a backup of previous databases and to publish these final results in 01_CURRENT_DATABASE_FILES folder.')
    

if __name__ == "__main__":
    main()
    print()
    input("Press Enter to finish...")