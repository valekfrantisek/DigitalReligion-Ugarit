""" This file serves to provide csv files for different kinds of statistical analysis of the Ugaritic corpus. It also makes a backup of the previous statistical files. """

import os
import pandas as pd
import numpy as np
from collections import defaultdict
import shutil
import time

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
UGARIT_TEXTS_DATABASE_PATH = os.path.join(ROOT_PATH, 'UGARIT_TEXTS_DATABASE.csv')
STATS_PATH = os.path.join(ROOT_PATH, 'STATISTICS')

order_of_languages = ['Ugaritic', 'Akkadian', 'Sumerian', 'Hurrian', 'Hittite', 'Egyptian', 'Cypro-Minoan', 'Phoenician', 'Latin', 'unknown/unassigned']

ktu_classification = {
    1: 'Literary and Religious',
    2: 'Letters',
    3: 'Legal and Juridical',
    4: 'Economic',
    5: 'Scribal Excercises',
    6: 'Inscriptions',
    7: 'Unclassified etc.',
    8: 'Unclassified etc.',
    9: 'Unclassified etc.',
    10: 'Ugaritic in syllabic'
}

ktu_classification_str = {
    '1': 'Literary and Religious',
    '2': 'Letters',
    '3': 'Legal and Juridical',
    '4': 'Economic',
    '5': 'Scribal Excercises',
    '6': 'Inscriptions',
    '7': 'Unclassified etc.',
    '8': 'Unclassified etc.',
    '9': 'Unclassified etc.',
    '10': 'Ugaritic in syllabic'
}

ktu_classification_str_religenres = {
    '1': 'other religious',
    '2': 'Letters',
    '3': 'Legal and Juridical',
    '4': 'Economic',
    '5': 'Scribal Excercises',
    '6': 'Inscriptions',
    '7': 'other religious',
    '8': 'other religious',
    '9': 'other religious',
    '10': 'Ugaritic in syllabic'
}

ktu_classification_str_religenres_related = {
    '1': 'Literary and Religious',
    '2': 'Letters',
    '3': 'Legal and Juridical',
    '4': 'Economic',
    '5': 'Scribal Excercises',
    '6': 'Inscriptions',
    '7': 'Unclassified etc.',
    '8': 'Unclassified etc.',
    '9': 'Unclassified etc.',
    '10': 'Ugaritic in syllabic'
}


def normalise_lang(input_lang, use_multilingual=False):
    """ This serves to "normalise" and simplify language attribution (e.g., from Akkadian? --> Akkadian)"""
    
    if type(input_lang) == float:
        return 'unknown/unassigned'
    
    langs_in_entry = input_lang.split(';')
    normalised_langs = []
    for lang in langs_in_entry:
        while '[' in lang or ']' in lang or '?' in lang or '(' in lang or ')' in lang or ' ' in lang:
            lang = lang.replace('[', '')
            lang = lang.replace(']', '')
            lang = lang.replace('?', '')
            lang = lang.replace('(', '')
            lang = lang.replace(')', '')
            lang = lang.replace(' ', '')
        
        if lang not in normalised_langs:
            normalised_langs.append(lang)
            
    if use_multilingual and len(normalised_langs) > 1:
        return 'multilingual'
    
    else:
        final_lang_string = ''
        for lang in order_of_languages:
            if lang in normalised_langs:
                final_lang_string += f';{lang}'
            else:
                continue
            
    return final_lang_string[1:]


def get_all_lang_combinations(input_db):
    all_lang_combinations = []

    for row_id in db.index:
        languages = normalise_lang(db.loc[row_id]['UTDB Language'], use_multilingual=False)
            
        all_lang_combinations.append(languages)
        
    all_lang_combinations = list(set(all_lang_combinations))
    all_lang_combinations.sort()
    # print(all_lang_combinations)
    return all_lang_combinations


def list_languages_sau_et_rsti(input_db, outfile_name:str, use_multilingual=True, ignore_langs=['Latin ("RSTI")', 'Phoenician ("RSTI")', 'Latin (SAU)', 'Phoenician (SAU)']):
    languages_counts = defaultdict(int)

    for row_id in input_db.index:
        languages_rsti = normalise_lang(input_db.loc[row_id]['RSTI Language'], use_multilingual=use_multilingual)
        languages_rsti = f'{languages_rsti} ("RSTI")'

        sau_languages = normalise_lang(input_db.loc[row_id]['SAU Language'], use_multilingual=use_multilingual)
        sau_languages = f'{sau_languages} (SAU)'
            
        if languages_rsti in ignore_langs or sau_languages in ignore_langs:
            continue
        else:
            languages_counts[sau_languages] += 1
            languages_counts[languages_rsti] += 1
            
    out_dict = {}
    for i, lang in enumerate(languages_counts):
        #print(lang, languages_counts[lang])
        out_dict[i] = {'language': lang, 'count of texts': languages_counts[lang]}
        if 'RSTI' in lang:
            out_dict[i]['RSTI_count'] = languages_counts[lang]
        else:
            out_dict[i]['RSTI_count'] = languages_counts[lang.replace('SAU', '"RSTI"')]

    df = pd.DataFrame.from_dict(out_dict)
    df = df.transpose()
    df.to_csv(os.path.join(STATS_PATH, outfile_name), sep=',', encoding='utf-8')


def list_locations_et_languages(input_db:pd.core.frame.DataFrame, outfile_name: str, ignore_langs=['Latin', 'Phoenician'], use_multilingual=True):
    """
    This function counts languages that are associated with individual archives (only at Ugarit).

    Args:
        input_db (pd.core.frame.DataFrame): pandas dataframe from which the data are extracted
        outfile_name (str): csv output filename (incl. ".cvs"), the file will be saved to STATS_PATH.
        ignore_langs (list): What languages should be ignored from the analysis (only when appearing independently!!)
    """
    
    locations_et_languages = defaultdict(dict)

    for row_id in input_db.index:
        languages = normalise_lang(input_db.loc[row_id]['UTDB Language'], use_multilingual=use_multilingual)
            
        general_area = input_db.loc[row_id]['Archive/General area']
        if type(general_area) == float:
            general_area = input_db.loc[row_id]['SAU Archive/General area']
            if type(general_area) == float:
                general_area = 'Other/unknown'


        if locations_et_languages[general_area]:
            locations_et_languages[general_area][languages] += 1
        else:
            locations_et_languages[general_area] = defaultdict(int)
            locations_et_languages[general_area][languages] += 1

    row_mockup = {'Akkadian': 0, 
                  'Ugaritic': 0, 
                  'Hurrian': 0, 
                  'Egyptian': 0, 
                  'Cypro-Minoan': 0, 
                  'Hittite': 0, 
                  'Sumerian': 0, 
                  'multilingual': 0, 
                  'unknown/unassigned': 0, 
                  'total': 0}
    
    if not use_multilingual:
        row_mockup = {}
        all_langs = get_all_lang_combinations(input_db=input_db)
        for lng_comb in all_langs:
            row_mockup[lng_comb] = 0
        row_mockup['total'] = 0

    # NOTE: list locations (as they appear in the DB under "(SUA) Archive/General area" column)
    locations_to_count_in = ['Royal Palace', 
                             'House of Urtenu', 
                             'House of Rapanu', 
                             'House of Yabninu', 
                             'Lamaštu', 
                             'House of the Literary Tablets', 
                             'Between Royal Palace and South Palace', 
                             'House of the High Priest', 
                             'House of Rašapabu', 
                             'Literate’s House', 
                             'House of the Hurrian Priest',
                             ]
    
    # Create first entry to the output dictionary, prepared to include all other locations.
    out_dict = {'Other/unknown': row_mockup.copy()}
    
    for location in locations_et_languages:
        #print(location)
        total = 0
        row_dict = row_mockup.copy()
        for lang in locations_et_languages[location]:
            ignore = False
            for ig_lang in ignore_langs:
                if lang == ig_lang:
                    ignore = True
                    
            if ignore:
                continue
            else:
                #print('\t', lang, locations_et_languages[location][lang])
                total += locations_et_languages[location][lang]
                if location in locations_to_count_in:
                    row_dict[lang] = locations_et_languages[location][lang]
                elif location == 'Ras Ibn-Hani' or location == 'Minet el-Beida' or location == 'Minet el Beida' or location == 'outside':
                    continue
                else:
                    out_dict['Other/unknown'][lang] += locations_et_languages[location][lang]
        
        if location in locations_to_count_in:
            row_dict['total'] = total
            out_dict[location] = row_dict

        #print('\t\tTotal:', total)

    output_db = pd.DataFrame.from_dict(out_dict)
    output_db = output_db.transpose()
    output_db.to_csv(os.path.join(STATS_PATH, outfile_name), sep=',', encoding='utf-8')


def compare_languages_in_RSTI_et_SAU(input_db:pd.core.frame.DataFrame, outfile_name='SAU_RSTI_lang_flow.csv', use_multilingual=False, ignore_langs=None):
    """
    This function compares information on languages as provided by RSTI (+-) or SAU.
    """
    
    from_to_dict = {}

    for row_id in input_db.index:
        SAU_lang = normalise_lang(input_db.loc[row_id]['SAU Language'], use_multilingual=use_multilingual)
        RSTI_lang = normalise_lang(input_db.loc[row_id]['RSTI Language'], use_multilingual=use_multilingual)
        
        from_to = f'{SAU_lang}--to--{RSTI_lang}'
        
        try:
            from_to_dict[from_to] += 1
        except:
            from_to_dict[from_to] = 1
            
    out_dict = {}
    idx = 0
    for key in from_to_dict:
        SAU_lang = key.split('--to--')[0]
        RSTI_lang = key.split('--to--')[1]
        
        if ignore_langs:
            if SAU_lang in ignore_langs or RSTI_lang in ignore_langs:
                continue
            else:
                out_dict[idx] = {'SAU language': SAU_lang, '"RSTI" language': RSTI_lang, 'value': from_to_dict[key]}
                
                idx += 1
        else:
            out_dict[idx] = {'SAU language': SAU_lang, '"RSTI" language': RSTI_lang, 'value': from_to_dict[key]}
        
            idx += 1

    output_db = pd.DataFrame.from_dict(out_dict)
    output_db = output_db.transpose()
    output_db.to_csv(os.path.join(STATS_PATH, outfile_name), sep=',', encoding='utf-8')


def list_languages_by_db(input_db, lang_by:str, outfile_name:str, use_multilingual=True, ignore_langs=['Latin', 'Phoenician'], only_multi=False):
    languages_counts = defaultdict(int)

    for row_id in input_db.index:
        if lang_by == 'utdb':
            languages = normalise_lang(input_db.loc[row_id]['UTDB Language'], use_multilingual=use_multilingual)
        elif lang_by == 'rsti':
            languages = normalise_lang(input_db.loc[row_id]['RSTI Language'], use_multilingual=use_multilingual)
        elif lang_by == 'sau':
            languages = normalise_lang(input_db.loc[row_id]['SAU Language'], use_multilingual=use_multilingual)
        else:
            print('WARNING: there is error in data selection setting, choose "utdb", "rsti" or "sau" for lang_by parameter.')
            return
        
        if not only_multi:
            if languages in ignore_langs:
                continue
            else:
                languages_counts[languages] += 1
        else:
            if languages in ignore_langs:
                continue
            elif len(languages.split(';')) >= 2:
                languages_counts[languages] += 1
            else:
                continue
    
    out_dict = {}
    for i, lang in enumerate(languages_counts):
        #print(lang, languages_counts[lang])
        out_dict[i] = {'language': lang, 'count of texts': languages_counts[lang]}

    df = pd.DataFrame.from_dict(out_dict)
    df = df.transpose()
    df.to_csv(os.path.join(STATS_PATH, outfile_name), sep=',', encoding='utf-8')
    
    
def list_languages(input_db, outfile_name:str, use_multilingual=True, ignore_langs=['Latin', 'Phoenician'], only_multi=False, only_Ugarit_MeB_RIH=True):
    languages_counts = defaultdict(int)

    for row_id in input_db.index:
        if only_Ugarit_MeB_RIH:
            if type(input_db.loc[row_id]['Excavation Siglum']) == float:
                continue
            
        languages = normalise_lang(input_db.loc[row_id]['UTDB Language'], use_multilingual=use_multilingual)
            
        if not only_multi:
            if languages in ignore_langs:
                continue
            else:
                languages_counts[languages] += 1
        else:
            if languages in ignore_langs:
                continue
            elif len(languages.split(';')) >= 2:
                languages_counts[languages] += 1
            else:
                continue
    
    out_dict = {}
    for i, lang in enumerate(languages_counts):
        #print(lang, languages_counts[lang])
        out_dict[i] = {'language': lang, 'count of texts': languages_counts[lang]}

    df = pd.DataFrame.from_dict(out_dict)
    df = df.transpose()
    df.to_csv(os.path.join(STATS_PATH, outfile_name), sep=',', encoding='utf-8')


def list_locations_et_ktu_genres(input_db, outfile_name:str):
    locations_et_ktu = defaultdict(dict)

    for row_id in input_db.index:
        general_area = input_db.loc[row_id]['Archive/General area']
        if type(general_area) == float:
            general_area = input_db.loc[row_id]['SAU Archive/General area']
            if type(general_area) == float:
                general_area = 'Other/unknown'
                
        ktu_num = str(input_db.loc[row_id]['KTU3'])
        
        if type(ktu_num) == float or ktu_num == 'nan':
            continue
        else:
            ktu_category = ktu_num.split('.')[0]

            if locations_et_ktu[general_area]:
                locations_et_ktu[general_area][ktu_category] += 1
            else:
                locations_et_ktu[general_area] = defaultdict(int)
                locations_et_ktu[general_area][ktu_category] += 1
    
    row_mockup = {'Literary and Religious': 0, 'Letters': 0, 'Legal and Juridical': 0, 'Economic': 0, 'Scribal Excercises': 0, 'Inscriptions': 0, 'Unclassified etc.': 0, 'Ugaritic in syllabic': 0, 'total': 0}
    locations_to_count_in = ['Royal Palace', 'House of Urtenu', 'House of Rapanu', 'House of Yabninu', 'Lamaštu', 'House of the Literary Tablets', 'Between Royal Palace and South Palace', 'House of the High Priest', 'House of Rašapabu', 'Literate’s House', 'House of the Hurrian Priest']

    out_dict = {'Other/unknown': row_mockup.copy()}

    for location in locations_et_ktu:
        #print(location)
        if location in locations_to_count_in:
            total = 0
            row_data = row_mockup.copy()
            for ktu in locations_et_ktu[location]:
                #print('\t', ktu, locations_et_ktu[location][ktu])            
                row_data[ktu_classification[int(ktu)]] += locations_et_ktu[location][ktu]
                total += locations_et_ktu[location][ktu]
            row_data['total'] = total
            #print('\t\tTotal:', total)
            out_dict[location] = row_data
        else:
            total = 0
            for ktu in locations_et_ktu[location]:
                #print('\t', ktu, locations_et_ktu[location][ktu])     
                out_dict['Other/unknown'][ktu_classification[int(ktu)]] += locations_et_ktu[location][ktu]
                total += locations_et_ktu[location][ktu]
            # Note that total is not added here this is so as the "other/unknown" data may be sorted in flourish to the last position
            #print('\t\tTotal:', total)

    df = pd.DataFrame.from_dict(out_dict)
    df = df.transpose()
    df.to_csv(os.path.join(STATS_PATH, outfile_name), sep=',', encoding='utf-8')


def get_religious_texts_stats(input_db, outfile_name:str, detailed_relation=False):
    locations_et_reli = defaultdict(dict)

    for row_id in input_db.index:
        general_area = input_db.loc[row_id]['Archive/General area']
        if type(general_area) == float:
            general_area = input_db.loc[row_id]['SAU Archive/General area']
            if type(general_area) == float or general_area == 'nan':
                general_area = 'Other/unknown'
        
        related_texts = input_db.loc[row_id]['"Clemens 2001"']
        if detailed_relation:
            out_dict = {'Other/unknown': {'religious': 0, 'related':0, 'probably related': 0, 'possibly related': 0, 'probably not related': 0, 'uncertain relation': 0, 'other': 0, 'total': 0}}
            if 'PRAVDA' == related_texts:
                relational_category = 'related'
            elif 'ADD TRUE' == related_texts:
                relational_category = 'related'
            elif 'PROBABLY TRUE' == related_texts:
                relational_category = 'probably related'
            elif 'POSSIBLY TRUE' == related_texts:
                relational_category = 'possibly related'
            elif 'UNCERTAIN' == related_texts:
                relational_category = 'uncertain relation'
            elif 'PROBABLY FALSE' == related_texts:
                relational_category = 'probably not related'
            elif 'SET TO FALSE' == related_texts:
                relational_category = False
            elif 'FALSE' == related_texts:
                relational_category = False
            else:
                relational_category = False
        else:
            out_dict = {'Other/unknown': {'religious': 0, 'related':0, 'other': 0, 'total': 0}}
            if related_texts in ['PRAVDA', 'ADD TRUE', 'PROBABLY TRUE', 'POSSIBLY TRUE']:
                relational_category = 'related'
            else:
                relational_category = False

        possible_relevant_genre = input_db.loc[row_id]['possible relevant (religious) genres']
        if type(possible_relevant_genre) == float or possible_relevant_genre == 'nan':
            possible_relevant_genre = False

        ktu_num = str(input_db.loc[row_id]['KTU3'])
        ktu_category = ktu_num.split('.')[0]

        if possible_relevant_genre or ktu_category == '1':
            if locations_et_reli[general_area]:
                locations_et_reli[general_area]['religious'] += 1
            else:
                locations_et_reli[general_area] = defaultdict(int)
                locations_et_reli[general_area]['religious'] += 1

        
        elif relational_category:
            if locations_et_reli[general_area]:
                locations_et_reli[general_area][relational_category] += 1
            else:
                locations_et_reli[general_area] = defaultdict(int)
                locations_et_reli[general_area][relational_category] += 1

        else:
            if locations_et_reli[general_area]:
                locations_et_reli[general_area]['other'] += 1
            else:
                locations_et_reli[general_area] = defaultdict(int)
                locations_et_reli[general_area]['other'] += 1
    
    locations_to_count_in = ['Royal Palace', 'House of Urtenu', 'House of Rapanu', 'House of Yabninu', 'Lamaštu', 'House of the Literary Tablets', 'Between Royal Palace and South Palace', 'House of the High Priest', 'House of Rašapabu', 'Literate’s House', 'House of the Hurrian Priest', 'Ras Ibn-Hani', 'Minet el-Beida']
    
    for location in locations_et_reli:
        #(location)
        if location in locations_to_count_in:
            row_data = {'religious': 0, 'other': 0, 'total': 0}
            for religious in locations_et_reli[location]:
                #print('\t', religious, locations_et_reli[location][religious])
                row_data[religious] = locations_et_reli[location][religious]
                row_data['total'] += locations_et_reli[location][religious]

            out_dict[location] = row_data
        else:
            row_data = out_dict['Other/unknown']
            for religious in locations_et_reli[location]:
                #print('\t', religious, locations_et_reli[location][religious])
                row_data[religious] += locations_et_reli[location][religious]

            out_dict['Other/unknown'] = row_data
            
    # NOTE: for ordering of the bars in Flourish:
    out_dict['Other/unknown']['total'] = 2
    out_dict['Ras Ibn-Hani']['total'] = 1
    out_dict['Minet el-Beida']['total'] = 0

    df = pd.DataFrame.from_dict(out_dict)
    df = df.transpose()
    df.to_csv(os.path.join(STATS_PATH, outfile_name), sep=',', encoding='utf-8')


def get_religious_texts_detailed_stats(input_db, outfile_name:str):
    locations_et_religenre = defaultdict(dict)

    for row_id in input_db.index:
        general_area = input_db.loc[row_id]['Archive/General area']
        if type(general_area) == float:
            general_area = input_db.loc[row_id]['SAU Archive/General area']
            if type(general_area) == float or general_area == 'nan':
                general_area = 'Other/unknown'
        
        related_texts = input_db.loc[row_id]['"Clemens 2001"']
        if related_texts in ['PRAVDA', 'ADD TRUE', 'PROBABLY TRUE', 'POSSIBLY TRUE']:
            relational_category = 'related'
        else:
            relational_category = False

        possible_relevant_genre = input_db.loc[row_id]['possible relevant (religious) genres']
        if type(possible_relevant_genre) == float or possible_relevant_genre == 'nan':
            possible_relevant_genre = False
        
        genre_according_to_sau = input_db.loc[row_id]['SAU genre']
        if type(genre_according_to_sau) == float or genre_according_to_sau == 'nan':
            genre_according_to_sau = False

        if possible_relevant_genre:
            if 'ritual' in possible_relevant_genre or  'sacrifices' in possible_relevant_genre or 'offerings' in possible_relevant_genre:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['ritual'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['ritual'] += 1
            elif  'myth' in possible_relevant_genre or  'epic' in possible_relevant_genre or 'wisdom' in possible_relevant_genre or 'literary' in possible_relevant_genre or 'narrative' in possible_relevant_genre:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['narrative'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['narrative'] += 1
            elif  'hymn' in possible_relevant_genre or  'prayer' in possible_relevant_genre:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['hymn/prayer'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['hymn/prayer'] += 1
            elif 'incantation' in possible_relevant_genre or 'magic' in possible_relevant_genre or 'medical' in possible_relevant_genre:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['incantation/magic'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['incantation/magic'] += 1
            elif  'omen' in possible_relevant_genre or  'divination' in possible_relevant_genre or 'oracular' in possible_relevant_genre or 'divinatiory' in possible_relevant_genre:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['divination'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['divination'] += 1
            else:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['other religious'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['other religious'] += 1

        elif genre_according_to_sau:
            if 'D' in genre_according_to_sau or 'liv' in genre_according_to_sau and 'Diri' not in genre_according_to_sau:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['divination'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['divination'] += 1
            elif 'M' in genre_according_to_sau or 'Lam' in genre_according_to_sau or 'Med' in genre_according_to_sau and 'Mat' not in genre_according_to_sau and 'Mea' not in genre_according_to_sau:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['incantation/magic'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['incantation/magic'] += 1                
            elif 'Lit' in genre_according_to_sau:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['narrative'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['narrative'] += 1
            elif 'Rel' in genre_according_to_sau or 'G' in genre_according_to_sau and 'RSGT' not in genre_according_to_sau:
                if locations_et_religenre[general_area]:
                    locations_et_religenre[general_area]['other religious'] += 1
                else:
                    locations_et_religenre[general_area] = defaultdict(int)
                    locations_et_religenre[general_area]['other religious'] += 1
            else:
                if relational_category:
                    if locations_et_religenre[general_area]:
                        locations_et_religenre[general_area]['related'] += 1
                    else:
                        locations_et_religenre[general_area] = defaultdict(int)
                        locations_et_religenre[general_area]['related'] += 1
            
        elif relational_category:
            if locations_et_religenre[general_area]:
                locations_et_religenre[general_area]['related'] += 1
            else:
                locations_et_religenre[general_area] = defaultdict(int)
                locations_et_religenre[general_area]['related'] += 1
        
        else:
            continue

    row_mockup = {'ritual': 0, 'narrative': 0, 'hymn/prayer': 0, 'divination': 0, 'incantation/magic': 0, 'other religious': 0, 'related': 0, 'total': 0}

    out_dict = {'Other/unknown': row_mockup.copy()} # The totals are for Flourish ordering of bars.
    locations_to_count_in = ['Royal Palace', 'House of Urtenu', 'House of Rapanu', 'House of Yabninu', 'Lamaštu', 'House of the Literary Tablets', 'Between Royal Palace and South Palace', 'House of the High Priest', 'House of Rašapabu', 'Literate’s House', 'House of the Hurrian Priest', 'Ras Ibn-Hani', 'Minet el-Beida']
    
    for location in locations_et_religenre:
        #print(location)
        if location in locations_to_count_in:
            row_data = row_mockup.copy()
            for religious in locations_et_religenre[location]:
                #print('\t', religious, locations_et_religenre[location][religious])
                row_data[religious] = locations_et_religenre[location][religious]
                row_data['total'] += locations_et_religenre[location][religious]

            out_dict[location] = row_data
        else:
            row_data = out_dict['Other/unknown']
            for religious in locations_et_religenre[location]:
                #print('\t', religious, locations_et_religenre[location][religious])
                row_data[religious] += locations_et_religenre[location][religious]

            out_dict['Other/unknown'] = row_data

    # NOTE: for ordering of the bars in Flourish:
    out_dict['Other/unknown']['total'] = 2
    out_dict['Ras Ibn-Hani']['total'] = 1
    out_dict['Minet el-Beida']['total'] = 0

    df = pd.DataFrame.from_dict(out_dict)
    df = df.transpose()
    df.to_csv(os.path.join(STATS_PATH, outfile_name), sep=',', encoding='utf-8')


def religious_languages(input_db, outfile_name:str, use_multilingual=True, ignore_langs=['Latin', 'Phoenician']):
    locations_languages = defaultdict(dict)

    for row_id in input_db.index:
        general_area = input_db.loc[row_id]['Archive/General area']
        if type(general_area) == float:
            general_area = input_db.loc[row_id]['SAU Archive/General area']
            if type(general_area) == float or general_area == 'nan':
                general_area = 'Other/unknown'

        languages = normalise_lang(input_db.loc[row_id]['UTDB Language'], use_multilingual=use_multilingual)
        
        related_texts = input_db.loc[row_id]['"Clemens 2001"']
        if related_texts in ['PRAVDA', 'ADD TRUE', 'PROBABLY TRUE', 'POSSIBLY TRUE']:
            relational_category = 'related'
        else:
            relational_category = False

        possible_relevant_genre = input_db.loc[row_id]['possible relevant (religious) genres']
        if type(possible_relevant_genre) == float or possible_relevant_genre == 'nan':
            add_religious_entry = False
        else:
            add_religious_entry = True
            
        genre_according_to_sau = input_db.loc[row_id]['SAU genre']
        if genre_according_to_sau == 'nan' or type(genre_according_to_sau) == float:
            genre_according_to_sau = 'None'
        if not add_religious_entry:
            if 'D' in genre_according_to_sau or 'liv' in genre_according_to_sau and 'Diri' not in genre_according_to_sau:
                add_religious_entry = True
            elif 'M' in genre_according_to_sau or 'Lam' in genre_according_to_sau or 'Med' in genre_according_to_sau and 'Mat' not in genre_according_to_sau and 'Mea' not in genre_according_to_sau:
                add_religious_entry = True             
            elif 'Lit' in genre_according_to_sau:
                add_religious_entry = True
            elif 'Rel' in genre_according_to_sau or 'G' in genre_according_to_sau and 'RSGT' not in genre_according_to_sau:
                add_religious_entry = True
            else:
                add_religious_entry = False

        if add_religious_entry:
            if locations_languages[general_area]:
                locations_languages[general_area][languages] += 1
            else:
                locations_languages[general_area] = defaultdict(int)
                locations_languages[general_area][languages] += 1

        elif relational_category:
            if locations_languages[general_area]:
                locations_languages[general_area][f'related {languages}'] += 1
            else:
                locations_languages[general_area] = defaultdict(int)
                locations_languages[general_area][f'related {languages}'] += 1
        
        else:
            continue

    row_mockup = {'Ugaritic': 0, 'Akkadian': 0, 'Hurrian': 0, 'Sumerian': 0, 'Hittite': 0, 'multilingual': 0, 'unknown/unassigned': 0, 'related Ugaritic': 0, 'related Akkadian': 0, 'related Sumerian':0, 'related Hittite': 0, 'related multilingual': 0, 'related unknown/unassigned': 0}
    row_mockup['total'] = 0

    out_dict = {'Other/unknown': row_mockup.copy()}
    locations_to_count_in = ['Royal Palace', 'House of Urtenu', 'House of Rapanu', 'House of Yabninu', 'Lamaštu', 'House of the Literary Tablets', 'Between Royal Palace and South Palace', 'House of the High Priest', 'House of Rašapabu', 'Literate’s House', 'House of the Hurrian Priest']
    
    for location in locations_languages:
        #print(location)
        if location in locations_to_count_in:
            row_data = row_mockup.copy()
            for lang in locations_languages[location]:
                if lang in ignore_langs:
                    continue
                else:
                    #print('\t', lang, locations_languages[location][lang])
                    row_data[lang] = locations_languages[location][lang]
                    row_data['total'] += locations_languages[location][lang]

            out_dict[location] = row_data
        else:
            row_data = out_dict['Other/unknown']
            for lang in locations_languages[location]:
                if lang in ignore_langs:
                    continue
                else:
                    #print('\t', lang, locations_languages[location][lang])
                    row_data[lang] += locations_languages[location][lang]

            out_dict['Other/unknown'] = row_data
    
    df = pd.DataFrame.from_dict(out_dict)
    df = df.transpose()
    df.to_csv(os.path.join(STATS_PATH, outfile_name), sep=',', encoding='utf-8')


def list_genres_in_cluster(input_db, cluster:str, outfile_name_prefix:str):    
    religenres = defaultdict(dict)

    for row_id in input_db.index:
        general_area = input_db.loc[row_id]['Archive/General area']
        if type(general_area) == float:
            general_area = input_db.loc[row_id]['SAU Archive/General area']
            if type(general_area) == float or general_area == 'nan':
                general_area = 'Other/unknown'
        
        if general_area == cluster:            
            related_texts = input_db.loc[row_id]['"Clemens 2001"']
            if related_texts in ['PRAVDA', 'ADD TRUE', 'PROBABLY TRUE', 'POSSIBLY TRUE']:
                relational_category = 'related'
            else:
                relational_category = False

            possible_relevant_genre = input_db.loc[row_id]['possible relevant (religious) genres']
            if type(possible_relevant_genre) == float or possible_relevant_genre == 'nan':
                possible_relevant_genre = False
            
            genre_according_to_sau = input_db.loc[row_id]['SAU genre']
            if type(genre_according_to_sau) == float or genre_according_to_sau == 'nan':
                genre_according_to_sau = False

            ktu_num = str(input_db.loc[row_id]['KTU3'])
            ktu_category = ktu_num.split('.')[0]

            if possible_relevant_genre:
                if 'ritual' in possible_relevant_genre or  'sacrifices' in possible_relevant_genre or 'offerings' in possible_relevant_genre:
                    if religenres['ritual']:
                        religenres['ritual'] += 1
                    else:
                        religenres['ritual'] = 1
                elif  'myth' in possible_relevant_genre or  'epic' in possible_relevant_genre or 'wisdom' in possible_relevant_genre or 'literary' in possible_relevant_genre or 'narrative' in possible_relevant_genre:
                    if religenres['narrative']:
                        religenres['narrative'] += 1
                    else:
                        religenres['narrative'] = 1
                elif  'hymn' in possible_relevant_genre or  'prayer' in possible_relevant_genre:
                    if religenres['hymn/prayer']:
                        religenres['hymn/prayer'] += 1
                    else:
                        religenres['hymn/prayer'] = 1
                elif 'incantation' in possible_relevant_genre or 'magic' in possible_relevant_genre or 'medical' in possible_relevant_genre:
                    if religenres['incantation/magic']:
                        religenres['incantation/magic'] += 1
                    else:
                        religenres['incantation/magic'] = 1
                elif  'omen' in possible_relevant_genre or  'divination' in possible_relevant_genre or 'oracular' in possible_relevant_genre or 'divinatiory' in possible_relevant_genre:
                    if religenres['divination']:
                        religenres['divination'] += 1
                    else:
                        religenres['divination'] = 1
                else:
                    try:
                        ktu_category_classification = ktu_classification_str_religenres[ktu_category]
                        if religenres[ktu_category_classification]:
                            religenres[ktu_category_classification] += 1
                        else:
                            religenres[ktu_category_classification] = 1
                    except:
                        if religenres['other religious']:
                            religenres['other religious'] += 1
                        else:
                            religenres['other religious'] = 1

            elif genre_according_to_sau:
                if 'D' in genre_according_to_sau or 'liv' in genre_according_to_sau and 'Diri' not in genre_according_to_sau:
                    if religenres['divination']:
                        religenres['divination'] += 1
                    else:
                        religenres['divination'] = 1
                elif 'M' in genre_according_to_sau or 'Lam' in genre_according_to_sau or 'Med' in genre_according_to_sau and 'Mat' not in genre_according_to_sau and 'Mea' not in genre_according_to_sau:
                    if religenres['incantation/magic']:
                        religenres['incantation/magic'] += 1
                    else:
                        religenres['incantation/magic'] = 1                
                elif 'Lit' in genre_according_to_sau:
                    if religenres['narrative']:
                        religenres['narrative'] += 1
                    else:
                        religenres['narrative'] = 1
                elif 'Rel' in genre_according_to_sau or 'G' in genre_according_to_sau and 'RSGT' not in genre_according_to_sau:
                    if religenres['other religious']:
                        religenres['other religious'] += 1
                    else:
                        religenres['other religious'] = 1
                else:
                    if relational_category:
                        try:
                            ktu_category_classification = ktu_classification_str_religenres_related[ktu_category]
                            if religenres[f"related {ktu_category_classification.replace('?', '')}"]:
                                religenres[f"related {ktu_category_classification.replace('?', '')}"] += 1
                            else:
                                religenres[f"related {ktu_category_classification.replace('?', '')}"] = 1
                        except:
                            if religenres[f"related {ktu_category_classification.replace('?', '')}"]:
                                religenres[f"related {ktu_category_classification.replace('?', '')}"] += 1
                            else:
                                religenres[f"related {ktu_category_classification.replace('?', '')}"] = 1

            
            elif relational_category:
                try:
                    ktu_category_classification = ktu_classification_str_religenres_related[ktu_category]
                    if religenres[f"related {ktu_category_classification.replace('?', '')}"]:
                        religenres[f"related {ktu_category_classification.replace('?', '')}"] += 1
                    else:
                        religenres[f"related {ktu_category_classification.replace('?', '')}"] = 1
                except:
                    if religenres[f"related {ktu_category_classification.replace('?', '')}"]:
                        religenres[f"related {ktu_category_classification.replace('?', '')}"] += 1
                    else:
                        religenres[f"related {ktu_category_classification.replace('?', '')}"] = 1

            
            else:
                continue
        else:
            continue

    out_dict = {}
    #out_dict['total'] = 0
    
    for religious in religenres:
        #print('\t', religious, religenres[religious])
        out_dict[religious] = religenres[religious]
        #out_dict['total'] += religenres[religious]

    df = pd.DataFrame.from_dict({0: out_dict})
    #df = df.transpose()
    df.to_csv(os.path.join(STATS_PATH, f'{outfile_name_prefix}_{cluster.replace(" ", "-")}.csv'), sep=',', encoding='utf-8')


def list_genres_detailes_in_cluster(input_db, outfile_name_prefix:str, cluster='Royal Palace'):
    religenres = defaultdict(dict)

    for row_id in input_db.index:
        general_area = input_db.loc[row_id]['Archive/General area']
        if type(general_area) == float:
            general_area = input_db.loc[row_id]['SAU Archive/General area']
            if type(general_area) == float or general_area == 'nan':
                general_area = 'Other/unknown'
        
        if general_area == cluster:
            detail_in_cluster = input_db.loc[row_id]['Detail in General']
            if type(detail_in_cluster) == float:
                detail_in_cluster = input_db.loc[row_id]['SAU Detail in General']
                if type(detail_in_cluster) == float or detail_in_cluster == 'nan':
                    detail_in_cluster = 'Other/unknown'
                    
            if not religenres[detail_in_cluster]:
                religenres[detail_in_cluster] = {'ritual': 0, 'narrative': 0, 'hymn/prayer': 0, 'divination': 0, 'incantation/magic': 0, 'other religious': 0, 'Economic': 0, 'related Economic': 0, 'related Legal and Juridical': 0, 'related Letters': 0, 'related Scribal Excercises': 0, 'related Inscriptions': 0, 'related other': 0, 'total': 0}
            
            related_texts = input_db.loc[row_id]['"Clemens 2001"']
            if related_texts in ['PRAVDA', 'ADD TRUE', 'PROBABLY TRUE', 'POSSIBLY TRUE']:
                relational_category = 'related'
            else:
                relational_category = False

            possible_relevant_genre = input_db.loc[row_id]['possible relevant (religious) genres']
            if type(possible_relevant_genre) == float or possible_relevant_genre == 'nan':
                possible_relevant_genre = False
            
            genre_according_to_sau = input_db.loc[row_id]['SAU genre']
            if type(genre_according_to_sau) == float or genre_according_to_sau == 'nan':
                genre_according_to_sau = False

            ktu_num = str(input_db.loc[row_id]['KTU3'])
            ktu_category = ktu_num.split('.')[0]

            religenres[detail_in_cluster]['total'] += 1 # This will be subtracted if the requirements are not satisfied.
            if possible_relevant_genre:
                if 'ritual' in possible_relevant_genre or  'sacrifices' in possible_relevant_genre or 'offerings' in possible_relevant_genre:
                    if religenres[detail_in_cluster]['ritual']:
                        religenres[detail_in_cluster]['ritual'] += 1
                    else:
                        religenres[detail_in_cluster]['ritual'] = 1
                elif  'myth' in possible_relevant_genre or  'epic' in possible_relevant_genre or 'wisdom' in possible_relevant_genre or 'literary' in possible_relevant_genre or 'narrative' in possible_relevant_genre:
                    if religenres[detail_in_cluster]['narrative']:
                        religenres[detail_in_cluster]['narrative'] += 1
                    else:
                        religenres[detail_in_cluster]['narrative'] = 1
                elif  'hymn' in possible_relevant_genre or  'prayer' in possible_relevant_genre:
                    if religenres[detail_in_cluster]['hymn/prayer']:
                        religenres[detail_in_cluster]['hymn/prayer'] += 1
                    else:
                        religenres[detail_in_cluster]['hymn/prayer'] = 1
                elif 'incantation' in possible_relevant_genre or 'magic' in possible_relevant_genre or 'medical' in possible_relevant_genre:
                    if religenres[detail_in_cluster]['incantation/magic']:
                        religenres[detail_in_cluster]['incantation/magic'] += 1
                    else:
                        religenres[detail_in_cluster]['incantation/magic'] = 1
                elif  'omen' in possible_relevant_genre or  'divination' in possible_relevant_genre or 'oracular' in possible_relevant_genre or 'divinatiory' in possible_relevant_genre:
                    if religenres[detail_in_cluster]['divination']:
                        religenres[detail_in_cluster]['divination'] += 1
                    else:
                        religenres[detail_in_cluster]['divination'] = 1
                else:
                    try:
                        ktu_category_classification = ktu_classification_str[ktu_category]
                        if religenres[detail_in_cluster][ktu_category_classification]:
                            religenres[detail_in_cluster][ktu_category_classification] += 1
                        else:
                            religenres[detail_in_cluster][ktu_category_classification] = 1
                    except:
                        if religenres[detail_in_cluster]['other religious']:
                            religenres[detail_in_cluster]['other religious'] += 1
                        else:
                            religenres[detail_in_cluster]['other religious'] = 1

            elif genre_according_to_sau:
                if 'D' in genre_according_to_sau or 'liv' in genre_according_to_sau and 'Diri' not in genre_according_to_sau:
                    if religenres[detail_in_cluster]['divination']:
                        religenres[detail_in_cluster]['divination'] += 1
                    else:
                        religenres[detail_in_cluster]['divination'] = 1
                elif 'M' in genre_according_to_sau or 'Lam' in genre_according_to_sau or 'Med' in genre_according_to_sau and 'Mat' not in genre_according_to_sau and 'Mea' not in genre_according_to_sau:
                    if religenres[detail_in_cluster]['incantation/magic']:
                        religenres[detail_in_cluster]['incantation/magic'] += 1
                    else:
                        religenres[detail_in_cluster]['incantation/magic'] = 1                
                elif 'Lit' in genre_according_to_sau:
                    if religenres[detail_in_cluster]['narrative']:
                        religenres[detail_in_cluster]['narrative'] += 1
                    else:
                        religenres[detail_in_cluster]['narrative'] = 1
                elif 'Rel' in genre_according_to_sau or 'G' in genre_according_to_sau and 'RSGT' not in genre_according_to_sau:
                    if religenres[detail_in_cluster]['other religious']:
                        religenres[detail_in_cluster]['other religious'] += 1
                    else:
                        religenres[detail_in_cluster]['other religious'] = 1
                else:
                    if relational_category:
                        try:
                            ktu_category_classification = ktu_classification_str[ktu_category]
                            if religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"]:
                                religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"] += 1
                            else:
                                religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"] = 1
                        except:
                            if religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"]:
                                religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"] += 1
                            else:
                                religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"] = 1
                                
                    else:
                        religenres[detail_in_cluster]['total'] -= 1

            
            elif relational_category:
                try:
                    ktu_category_classification = ktu_classification_str[ktu_category]
                    if religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"]:
                        religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"] += 1
                    else:
                        religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"] = 1
                except:
                    if religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"]:
                        religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"] += 1
                    else:
                        religenres[detail_in_cluster][f"related {ktu_category_classification.replace('?', '')}"] = 1

            
            else:
                religenres[detail_in_cluster]['total'] -= 1
                continue
        else:
            continue

    religenres['Other/unknown']['total'] = 0

    df = pd.DataFrame.from_dict(religenres)
    df = df.transpose()
    df.to_csv(os.path.join(STATS_PATH, f'{outfile_name_prefix}_{cluster.replace(" ", "-")}.csv'), sep=',', encoding='utf-8')


def make_backup_of_stats():
    BACKUPT_TIMESTAMP = time.strftime("%d-%m-%Y--%H-%M", time.localtime())
    BACKUP_files_PATH_STATS = os.path.join(STATS_PATH, 'STATS_BACKUP', f'BACKUP_{BACKUPT_TIMESTAMP}')
    
    os.mkdir(BACKUP_files_PATH_STATS)

    for file_ in os.listdir(STATS_PATH):
        file_path = os.path.join(STATS_PATH, file_)

        if file_path.endswith('.csv'):
            shutil.copy(file_path, BACKUP_files_PATH_STATS)
    
    print()
    print('Backup has been made to directory BACKUP_'+BACKUPT_TIMESTAMP)
    
def detele_files():
    files_in_STATS = os.listdir(STATS_PATH)
    for file_csv in files_in_STATS:
        if os.path.isdir(os.path.join(STATS_PATH, file_csv)):
            continue
        else:
            os.remove(os.path.join(STATS_PATH, file_csv))
    
    print()
    print('Previous statistical data were cleared.')
    

if __name__ == "__main__":
    make_backup_of_stats()
    detele_files()
    
    db = pd.read_csv(UGARIT_TEXTS_DATABASE_PATH, encoding='utf-8', delimiter=';', dtype=str, index_col=0)
    
    """ Analysing languages in locations """
    print('Analysing languages in locations...')
    list_locations_et_languages(input_db=db, outfile_name='locations_et_languages.csv', use_multilingual=False)
    list_locations_et_languages(input_db=db, outfile_name='locations_et_languages_full.csv', use_multilingual=True)
    
    """ Comparing languages in SAU and RSTI """
    print('Cmparing languages in SAU and RSTI...')
    compare_languages_in_RSTI_et_SAU(db, outfile_name='SAU_RSTI_lang_flow.csv', use_multilingual=False, ignore_langs=None)
    compare_languages_in_RSTI_et_SAU(db, outfile_name='SAU_RSTI_lang_flow_multilingual.csv', use_multilingual=True)
    compare_languages_in_RSTI_et_SAU(db, outfile_name='SAU_RSTI_lang_flow_multilingual_ignoreLatEtPhoe.csv', use_multilingual=True, ignore_langs=['Latin', 'Phoenician'])
    
    """ General language statistics """
    print('Doing general language statistics...')    
    list_languages(input_db=db, outfile_name='language_statistics.csv')
    list_languages(input_db=db, outfile_name='language_statistics_multi.csv', only_multi=True, use_multilingual=False)
    
    """ KTU genres in locations """
    print('Analysing KTU classification in locations...')
    list_locations_et_ktu_genres(input_db=db, outfile_name='KTU_in_locations.csv')
    
    """ General religious situation analysis """
    print('Analysing general language situation...')
    get_religious_texts_stats(input_db=db, outfile_name='religious_texts_in_locations.csv', detailed_relation=False)

    get_religious_texts_stats(input_db=db, outfile_name='religious_texts_in_locations_detailed.csv', detailed_relation=True)
    
    """ Detailed religious situation analysis """
    print('Analysing detailed language situation...')
    get_religious_texts_detailed_stats(input_db=db, outfile_name='religious_texts_inlocs_detailed_genres.csv')
    
    """ Religious languages """
    print('Analysing language situation of religious texts...')
    religious_languages(input_db=db, outfile_name='religious_langs_stats.csv')
    
    """ Analysis of religious texts in clusters """
    print('Analysing religious texts in clusters...')
    list_genres_in_cluster(input_db=db, cluster='House of the High Priest', outfile_name_prefix='cluster_stats')
    list_genres_in_cluster(input_db=db, cluster='House of the Hurrian Priest', outfile_name_prefix='cluster_stats')
    
    """ Analysis of Religious texts in clusters - inner locations """
    print('Analysing religious texts in inner locations of clusters...')
    list_genres_detailes_in_cluster(input_db=db, outfile_name_prefix='detailed_stats')
    
    print()
    input("Press Enter to finish...")