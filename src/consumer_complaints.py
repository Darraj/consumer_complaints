#!/usr/bin/env python3
import os
import pandas as pd

# defining global variables
LOCALPATH = os.path.abspath(__file__).replace(os.path.basename(__file__), "")
INPUTFILE = os.path.join(LOCALPATH, "..", "input", "complaints.csv")
OUTPUTFILE = os.path.join(LOCALPATH, "..", "output", "report.csv")

# load only needed columns
df = pd.read_csv(
    INPUTFILE,
    usecols=[
        u'Date received',
        u'Product',
        u'Company'
    ]
)

# make product column all lower case
df.Product = df.Product.str.lower()

# covert date to year
df['year'] = pd.DatetimeIndex(df["Date received"]).year

# count number of complaints per product per year
agg_df = df.groupby(['Product', 'year']).agg(
    complaint_cnt=('Product', 'count'))
# agg_df.columns = agg_df.columns.droplevel(0)

# count number of companies receiving a complaint for that product per year
agg_df2 = df.groupby(['Product', 'year', 'Company']).agg(
    cmpny_cnt=('Company', 'count'))

# total company count per product per year
agg_df3 = df.groupby(['Product', 'year']).agg(cmpny_cnt=('Company', 'count'))

# # max number of complaints for a company per product per year
agg_df2 = agg_df2.groupby(['Product', 'year']).agg(
    max_cmpny_cnt=('cmpny_cnt', 'max'))

# Join aggregated columns to original agg_df
agg_df = agg_df.join(other=agg_df2, on=['Product', 'year'], how='inner')
agg_df = agg_df.join(other=agg_df3, on=['Product', 'year'], how='inner')
agg_df['max_cmpny_complaint_perc'] = round(
    agg_df.max_cmpny_cnt/agg_df.cmpny_cnt * 100)

# write result to file
agg_df[['complaint_cnt', 'max_cmpny_cnt',
        'max_cmpny_complaint_perc']].to_csv(OUTPUTFILE)
