'''This module contains main logic of the pipeline.'''

from src.ETL import extract, transform, load

def main():
    df1, df2, df3 = extract(['UCKq-lHnyradGRmFClX_ACMw'])
    final_df = transform(df1, df2, df3)
    load(final_df, 'monthly_metrics')

if __name__ == "__main__":
    main()