from data_scripts.get_data import AHistroy

data = AHistroy('all',Source='alpha',Start_Day='2020/01/01')
#conn = create_engine('postgresql+psycopg2://' + qtheus_rds['user'] + ':' + qtheus_rds['password'] + '@' + qtheus_rds['host'] + '/' +    qtheus_rds['dbname'])
#query = f"SELECT * FROM ticker_lists.tickers WHERE ticker = 'AAPL'"  # Construct query to get old data from database and clean it against the new data

#tickers = pandas.read_sql(query, conn)
pass