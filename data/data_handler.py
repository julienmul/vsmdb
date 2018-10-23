def get_jdk_data(tsid_list, beg_date='2010-01-01', frequency='daily',
                 data_vendor_id=20, verbose=False):
    from qsmdb import qsmdb
    df = qsmdb.get_security_prices(tsid_list, beg_date=beg_date, frequency=frequency,
                                   data_vendor_id=data_vendor_id, verbose=verbose)
    # drop redundant columns
    df = df.drop(['open', 'high', 'low', 'split', 'dividend', 'volume'], axis=1)
    # unstack df
    df = df.groupby(['date', 'tsid']).mean().unstack()
    df.columns = df.columns.droplevel()
    df.index.name = None
    df.columns.name = None
    return df


def get_jdk_matrix(input_df, input_series, window=10):
    """
    My interpretation of JdK Scores
    """
    from pandas import DataFrame, concat

    def rs_ratio(prices_df, benchmark, rwindow=10):
        new_prices_df = DataFrame()
        for series in prices_df:
            rs = (prices_df[series].divide(benchmark)) * 100
            rs_ratios = rs.rolling(rwindow).mean()
            rel_ratio = 100 + ((rs_ratios - rs_ratios.mean()) / rs_ratios.std() + 1)
            new_prices_df[series] = rel_ratio
        new_prices_df.dropna(axis=0, how='all', inplace=True)
        return new_prices_df

    def rs_momentum(prices_df, rwindow=10):
        new_prices_df = DataFrame()
        for series in prices_df:
            mom = (prices_df[series].pct_change()) * 100
            mom_smooth = mom.rolling(rwindow).mean()
            rel_mom = 100 + ((mom_smooth - mom_smooth.mean()) / mom_smooth.std() + 1)
            new_prices_df[series] = rel_mom
        new_prices_df.dropna(axis=0, how='all', inplace=True)
        return new_prices_df

    ratio_df = rs_ratio(input_df, input_series, window)
    momentum_df = rs_momentum(ratio_df, window)
    if len(ratio_df) != len(momentum_df):
        ratio_df.drop(ratio_df.index[:window], inplace=True)

    assert set(momentum_df.columns.values) == set(ratio_df.columns.values)
    combined_dict = {}
    for ticker in momentum_df:
        new_df = concat([momentum_df[ticker], ratio_df[ticker]], axis=1)
        new_df.columns = ['JDK_momentum', 'JDK_ratio']
        new_df = new_df.reset_index()
        new_df['index'] = new_df['index'].apply(lambda x: x.strftime('%Y-%m-%d'))
        new_dict = new_df.to_dict(orient='records')
        combined_dict[ticker] = new_dict
    return combined_dict
