library(dplyr)
library(tidyr)
library(ggplot2)
library(RMySQL)
library(zoo)
library(ggplot2)
library(stringr)
library(lubridate)

# 
# user ='root'
# password = 'StoneRanger12#'
# dbname = 'main_schema'
# host ='173.194.227.245'
# conn = dbConnect(MySQL(), user=user, password=password, dbname=dbname, host=host)


get_aligned_spread = function(tickers_all, ticker_weights, start_year, conn ){
  
  
  # --------------- Lookup underlying tickerrs and first notice periods for the contracts in the list
  # looks up main_schema.ticker_db for all tickerYY and notice periods matching those in the list
  
  
  first_ticker = tickers_all[[1]]
  first_weight = ticker_weights[[1]]
  
  ticker_first_notice_query =
    "
  Select * from 
  main_schema.ticker_db 
  where
  ( %s ) 
  and first_notice is not null;
  " 
  
  tickers_joined <- 
    tickers_all %>% 
    paste0("ticker like '",.,"__'") %>% 
    paste0(collapse=' or ') 
  
  sql <- 
    ticker_first_notice_query%>% 
    sprintf(tickers_joined)
  
  ticker_notice = dbSendQuery(conn,sql) %>% fetch(n=-1)
  
  # ------------ make mapping table just with tickers
  tickers_with_id <- ticker_notice %>% select(ticker, id)
  
  
  # ----- Make table with info for looking up underling data
  # for each tickerYY for first contract, make a row with:
  # first_notice_date - last day in lookup window for this ticker
  # start_date - first day in report window for this ticker
  # initialise_date - this is leading data to initialise things like moving average that will get dropped in final report
  # ticker_weights - this is a numeric weight or label that will be added as column for each ticker used for creating spreads
  
  pattern_ticker_first <- 
    first_ticker %>%
    paste0('^',.,"\\d{2}")
  
  tickers_others = tickers_all[-1]
  ticker_weights_others = ticker_weights[-1]
  
  lookup_table <- 
    ticker_notice %>% 
    filter(str_detect(ticker, pattern_ticker_first)) %>%
    filter(year>= start_year) %>%
    select(id, ticker, year, first_notice) %>%
    mutate(start_date = lag(first_notice,n=1)) %>%
    filter(!is.na(start_date )) %>%
    rowwise() %>%
    mutate(initialise_date = ymd(start_date) - months(1), charyear = str_sub(as.character(year),3,4)) %>%
    mutate(spread_weight = first_weight) 
  
  
  #Ugly way to make lookup table using loop
  #make ticker labels with contractYY, and rows matching first ticker lookup info
  #Append these iteratively to create table with all tickers and lookup info
  
  final_lookup_table = lookup_table
  
  
  for(i in 1:length(tickers_others)){
    this_ticker = tickers_others[i]
    this_weight = ticker_weights_others[i]
    
    final_lookup_table <- bind_rows(
      final_lookup_table,  
      lookup_table %>% 
        mutate(ticker = paste0(this_ticker,charyear)) %>%
        select(-id) %>%
        left_join(tickers_with_id, c("ticker"="ticker")) %>%
        mutate(spread_weight = this_weight) 
    )
  }
  
  # ------- Make SQL text query ----------------------
  
  
  #Using final lookup table create sql filter to pull each underlying contractYY and date range
  #Note that we have to actually get the contract_id to lookup the contractYY in the contract_db table
  
  underlying_filter <- final_lookup_table %>% 
    #next two lines groups contracts together by year and puts ids together in string for sql filter
    group_by(charyear) %>%
    mutate(filter = paste0(as.character(id), collapse=',')) %>%
    #Next step lines groups contracts by year and pastes the ids together with date range lookups
    group_by(charyear) %>%
    mutate(filter = paste0("( ticker_id in (",
                           filter,
                           ") and date <= '",
                           first_notice,
                           "' and date > '",
                           initialise_date,
                           "')")) %>%
    ungroup %>%  
    select(filter)
  
  #makes final query
  top_of_query = "select * from main_schema.contract_db where "
  query_filter_all = underlying_filter[[1]] %>% paste(collapse = " or ") %>% paste0(top_of_query,.)
  
  
  # ----------- Execute query -----------------------------------
  # gets data for both tickers
  
  resSet = dbSendQuery(conn,query_filter_all)
  data = resSet %>% fetch(n=-1)
  
  data2 <- data %>%
    left_join(final_lookup_table,c("ticker_id"="id"))
  
  conn %>% dbDisconnect()
  
  return(data2)
  
  
  
}

get_reference_db = function(conn, futures_only = TRUE){
  
  sql = "Select * from main_schema.reference_db"
  
  if(futures_only){
    sql = paste0(sql, " where type = 'Futures' ")
  }
  
  data <- sql %>% dbSendQuery(conn,.) %>% fetch(n=-1)
  conn %>% dbDisconnect()
  
  return(data)
  
}
