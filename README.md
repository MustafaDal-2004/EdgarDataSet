# EdgarDataSet
Python3 Script run script to build your own Edgar data set

1. open enter setbuild
2.  your email on the config section
3. run SetBuild.py this will go through the csv called sp500_yfin downloading all data for given ticker from 2015-2025
4. run SetUpdate.py this will scan 2026 or the entered year and update the data set

To keep most upto date data you can update as frequently as you wish using SetUpdate.py , packages used are pandas ,requests and os and as of now saves everything as csv if you wish to change to parquet you can do on SetBuilder and SetUpdate.py 

