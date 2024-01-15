""" This python script serves to make backup of the old published files, copy the current db files to the final destination (01_CURRENT_DATABASE_FILES), and transform them to .xlsx format. """

import os
import shutil
import time
import pandas as pd

ROOT_PATH = os.getcwd()

CSV_files_PATH = os.path.join(ROOT_PATH, '01_CURRENT_DATABASE_FILES', 'CSV')
EXCEL_files_PATH = os.path.join(ROOT_PATH, '01_CURRENT_DATABASE_FILES', 'EXCEL')


def save_backup():
    BACKUPT_TIMESTAMP = time.strftime("%d-%m-%Y--%H-%M", time.localtime())
    BACKUP_files_PATH_CSV = os.path.join(ROOT_PATH, '01_CURRENT_DATABASE_FILES', 'BACKUP', f'BACKUP_{BACKUPT_TIMESTAMP}', 'CSV')
    BACKUP_files_PATH_EXCEL = os.path.join(ROOT_PATH, '01_CURRENT_DATABASE_FILES', 'BACKUP', f'BACKUP_{BACKUPT_TIMESTAMP}', 'EXCEL')

    shutil.copytree(CSV_files_PATH, BACKUP_files_PATH_CSV)
    shutil.copytree(EXCEL_files_PATH, BACKUP_files_PATH_EXCEL)
    
    print('Backup has been made to directory BACKUP_'+BACKUPT_TIMESTAMP)
    
    
def move_files():
    """ First, clear the original """
    files_in_CSV = os.listdir(CSV_files_PATH)
    for file_csv in files_in_CSV:
        os.remove(os.path.join(CSV_files_PATH, file_csv))
    
    files_in_EXCEL = os.listdir(EXCEL_files_PATH)
    for file_excel in files_in_EXCEL:
        os.remove(os.path.join(EXCEL_files_PATH, file_excel))
        
    print('Old files have been removed.')
    
    """ Then, we move the updated files. """
    files_to_move = [
        'ENTRIES_IN_SAU_ASSOCIATED_WITH_MULTIPLE_UTDB_ENTRIES.csv',
        'ENTRIES_IN_UTDB_ASSOCIATED_WITH_MULTIPLE_SAU_ENTRIES.csv',
        'RSTI_NON_TEXTS_OBJECTS.csv',
        'SAU_NON_TEXTS_OBJECTS.csv',
        'SAU_COLLECTION.csv',
        'UGARIT_TEXTS_DATABASE.csv',
        'RSTI_MODIFIED.csv'
    ]
    
    for file_ in files_to_move:
        shutil.copyfile(os.path.join(ROOT_PATH, file_), os.path.join(CSV_files_PATH, file_))
        
    print('New files have been copied to the final destination.')
    
    
def csv_to_excel_with_formatting(csv_file:str, excel_file:str, header_color='#FFA500', odd_lines_color='#FFFFFF', even_lines_color='#FFF2E5'):
    df = pd.read_csv(csv_file, encoding='utf-8', sep=';')
    
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')

        workbook  = writer.book
        worksheet = writer.sheets['Data']

        header_format = workbook.add_format({
            'bg_color': header_color,
            'bold': True
        })

        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        row_format1 = workbook.add_format({'bg_color': odd_lines_color}) # Bílá
        row_format2 = workbook.add_format({'bg_color': even_lines_color}) # Jemná oranžová

        for row in range(1, len(df) + 1):
            row_format = row_format2 if row % 2 == 0 else row_format1
            for col in range(len(df.columns)):
                cell_value = df.iloc[row-1, col]
                if pd.isnull(cell_value):
                    worksheet.write_blank(row, col, '', row_format)
                else:
                    worksheet.write(row, col, cell_value, row_format)

        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        

if __name__ == "__main__":
    save_backup()
    move_files()
    
    files_in_csv = os.listdir(CSV_files_PATH)

    for csv_filename in files_in_csv:
        csv_file_PATH = os.path.join(CSV_files_PATH, csv_filename)
        excel_filename = csv_filename.replace('.csv', '.xlsx')
        excel_PATH = os.path.join(EXCEL_files_PATH, excel_filename)
        
        csv_to_excel_with_formatting(csv_file=csv_file_PATH, excel_file=excel_PATH)
        
        print(csv_filename, 'has been saved as', excel_filename)
    
    print()
    input("Press Enter to finish...")