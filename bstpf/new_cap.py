
import camelot
import pandas
import numpy as np

 
# Have the User Put in Pages to Process

def get_tables(file):

    fl_name = file.name

    tables = camelot.read_pdf(file, pages="80-140",flavor='stream')


    print(tables[26])


    df_main = pandas.DataFrame()

    for table in tables:
        df_main = pandas.concat([df_main, table.df], ignore_index=True)

    ##based on file name decide which function to use

 
# Julius Bar Logic

def convert_julius_bar(df_main, ):
    key_words = ['Direct Equity','Sell Date']


    r1 = df_main[0].value_counts()['Sell Date']


    for x in range(0,r1):
        start_label_index = df_main.index[df_main[0] == key_words[0]][0]
        end_label_index = df_main.index[df_main[0] == key_words[1]][0]
        start_pos = df_main.index.get_loc(start_label_index)
        end_pos = df_main.index.get_loc(end_label_index)

        end_pos = end_pos+3
        indices_to_drop = df_main.iloc[start_pos:end_pos].index

        df_main.drop(indices_to_drop, inplace=True)


    words = ["Vimuras Family Private Trust", "\\*Refer Disclaimer at the end of the report.","Equity","Taxable Gain & Loss Statement"]
    pattern = "|".join(words)

    
    # Check each row if it has the above if so then delete the row, proboly need to loop


    df_main = df_main[~df_main[0].str.contains(pattern, na=False, case=False)]
    df_main = df_main[~df_main[5].str.contains(pattern, na=False, case=False)]


    df_main.reset_index(inplace=True,drop=True)


    def is_comp(val):
        if not isinstance(val, str):
            return False

        # 2. Strip whitespace from the string. This is the key change.
        #    "   Company A   " -> "Company A"
        #    "       "         -> ""
        cleaned_val = val.strip()

        # 3. Check if the cleaned string is empty. If it is, it's not a company name.
        #    This now correctly handles strings that were originally just whitespace.
        if not cleaned_val:
            return False

        # 4. Now that we know we have a non-empty, cleaned string, apply the final rules.
        if cleaned_val.lower() == 'total' or cleaned_val[0].isdigit():
            return False

        # 5. If it passed all the checks, it's a company name.
        return True



    mask1 = df_main[0].apply(is_comp)
    mask2 = df_main[1].apply(is_comp)



    mask1.value_counts()


    combine_mask = mask1 | mask2


    company_names = pandas.Series(np.where(mask1, df_main[0], df_main[1]))


    company_col = company_names.where(combine_mask).ffill()


    df_main = df_main[~combine_mask]


    df_main.insert(0, "Company", company_col)


    df_main.columns = ["Company", "Sell Date", "Quantity", "Sell Rate", "Total Sale Value", "Purchase Date", "Purchase Rate","Actual Cost", "FMV as on 31-01-2018/Indexed Rate", "Applicable Rate", "Effective Cost", "Days Held", "Short Term", "Long Term", "Effective LT"]

    df_main.to_excel("")