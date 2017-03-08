# Description:

#get date range

#Get tickers
#The underlying tickers change each year


library(RMySQL)

ip = "173.194.227.245"
user = "root"
port = 3306
password = "StoneRanger12#"


mydb = dbConnect(MySQL(), user=user, password=password, dbname="main_schema", host=ip)




#get formula

#make spread

#calc sma n weeks
# note you need n weeks prior to start of i
#Get tickers
library(RODBC)
logdebug(paste0("opening connection to ",dsnConnectionName))
#odbcClose(ch)

#have to retry connecting to mysql ??
odbcClose(ch)
ch = -1

numtries = 1
while(ch == -1 && numtries < 20){
  ch <- odbcConnect(dsn = dsnConnectionName)
  numtries  = numtries + 1
}


# -------- Try getting data by using generic ticker
# No good, can get the generic JUL1 contract, but this kicks in at July 15 of the year
# We want to get date range for each year, JUL1 20xx - JUN30 20x(x+1) and n weeks prior histor
# will need to look up underlying contract db data instead

sql_query = "

select 
t1.generic_name,
t2.*    
from 
main_schema.generic_ticker_db t1
join
main_schema.generic_db t2
on
t1.id = t2.generic_ticker_id
where
t1.generic_name in ('W_JUL1','C_JUL1')
and
date > '2002'"

df_data = sqlQuery(ch,sql_query, stringAsFactors = FALSE)

head(df_data)

#below pivots out generic contract value and underlying ticker
#we can then compare this with what Mike is using
#see that data matches at july 15 of year
#we need to match from july 01 plus 4 prior weeks
df_cast_cont = dcast(df_data, "date ~ generic_name", value.var = c("current_contract"))
df_cast_close = dcast(df_data, "date ~ generic_name", value.var = c("close"))
df_cast_close$spread = df_cast_close$W_JUL1 - df_cast_close$C_JUL1
df_cast = merge(df_cast_cont, df_cast_close,by='date')

#---- search by underlying spread


sql_query =
  "
select 
t1.ticker, t1.year, t1.month,
t2.*
from 
ticker_db t1
join
contract_db t2
where
t1.id = t2.ticker_id
and
ticker in ('CN02','WN02')
and
date > '2001-06-01'
"


df_data = sqlQuery(ch,sql_query)
#query takes a while, think of refactor
head(df_data)

df_cast = dcast(df_data, "date ~ ticker", value.var = c("close"))
df_cast$spread = df_cast$WN02 - df_cast$CN02 

#Using datalayer
the_tickers[1][[1]]
#get long leg, filter time period and just close prices
df_long =getContractPrice(ch,'WN02',startDate = '2001-06-01')
df_long = df_long["2001-06/","close"]
#rename column
colnames(df_long) = 'leg1'

#get short leg, filter time period and just close prices
df_short = getContractPrice(ch,'WN02',startDate = '2001-06-01')
# rename column
df_short = df_short["2001-06/","close"]
colnames(df_short) = 'leg2'

#merge legs for making spread
df_spread = merge(df_long,df_short,all=T)
head(df_spread)

#Add in dates to fill in all dates and fill na's by last obs carried forward
dates = seq(as.Date("2001-06-01"), as.Date("2002-06-30"),by="day")
xts_1 = xts(rep(1,length(dates)),order.by = dates)
df_spread = merge(df_spread, xts_1, all=T)
df_spread = na.locf(df_spread)
df_spread$spread = df_spread$leg2 - df_spread$leg1

#functionify

getSpreadTwoLegs = function(ch, ticker1, ticker2, startDate, endDate, colnames = c('leg1','leg2')){
  
  #This function looks up two underlying contract by ticker from contract_db table in rangestone db
  #it merges into xts object, expands to have date incrementing by one day a fills NAs by last observation
  #carried forward
  #it also filters beetween dates
  #it then calculates the spread ticker1 - ticker2
  
  
  dateFilter = paste0(startDate,"/",endDate)
  
  #get long leg, filter time period and just close prices
  df_long =getContractPrice(ch, ticker1)
  df_long = df_long[dateFilter,"close"]
  #rename column
  colnames(df_long) = colnames[1]
  
  #get short leg, filter time period and just close prices
  df_short = getContractPrice(ch,ticker2)
  # rename column
  df_short = df_short[dateFilter,"close"]
  colnames(df_short) = colnames[2]
  
  #merge legs for making spread
  df_spread = merge(df_long,df_short,all=T)
  
  
  #Add in dates to fill in all dates and fill na's by last obs carried forward
  dates = seq(as.Date(startDate), as.Date(endDate),by="day")
  xts_1 = xts(rep(1,length(dates)),order.by = dates)
  df_spread = merge(df_spread, xts_1, all=T)
  df_spread = na.locf(df_spread)
  df_spread$spread = df_spread$leg1 - df_spread$leg2
  
  return(df_spread)
  
}



#now generate inputs to lookup
#The underlying tickers change each year
generic_tickers = c("WN","CN")
years_end = 2002:2017
years_start = 2001:2016

all_spreads = list()

for( i in 1:length(years_end)) {
  print(i)
  #generate tickers
  
  year_with_0 = substr(as.character(years_end[i]),3,4)
  the_tickers = sapply(generic_tickers,function(x){paste0(x, year_with_0)})
  
  #generate lookup date range
  start_lookup_date = paste0(years_start[i],"-06-01")
  end_lookup_date = paste0(years_end[i],"-06-30")
  
  #generate include date range
  start_include_date = paste0(years_start[i],"-07-01")
  end_include_date = paste0(years_end[i],"-06-30")
  
  
  #get data and calc ticker spread
  df_spread = getSpreadTwoLegs(ch,
                               ticker1 = the_tickers[1][[1]], 
                               ticker2 = the_tickers[2][[1]],
                               startDate = start_lookup_date,
                               endDate = end_lookup_date)
  
  #generate sma
  sma = rollapply(df_spread[,"spread"],width = 28, mean)
  #merge on via - seems constructor below handles index merge/align for xts
  df_spread$sma = sma
  df_spread$ma_series_spread = df_spread$spread - df_spread$sma
  #filter to just the year, ie exclued the intitialisation for the ma
  this_date_filter = paste0(start_include_date,"/",end_include_date)
  df_spread = df_spread[this_date_filter,]
  
  all_spreads[[i]] = df_spread
  #plot(df_spread[,"ma_series_spread"])
  #head(df_spread)
}


df_all_spreads = Reduce(rbind, all_spreads)
df_all_spreads['2017-02-10/',] = NA
tail(df_all_spreads)
nrow(df_all_spreads)

rank_spread = rank(as.vector(df_all_spreads$ma_series_spread),na.last=NA)
nrank = length(rank_spread)
percentile_spread = rank_spread/max(rank_spread)
length(percentile_spread)
df_all_spreads$rank = xts(rank_spread,order.by = index(df_all_spreads)[1:nrank])
df_all_spreads$percentile_spread = xts(percentile_spread,order.by = index(df_all_spreads)[1:nrank])


plot(df_all_spreads['2016-07-01/','spread'])
plot(df_all_spreads['2016-07-01/','percentile_spread'])




library(ggplot2)
library(reshape2)
tail(df_all_spreads)

df_plot = as.data.frame(df_all_spreads['2016-07-01/',c('spread','percentile_spread')])
#df_plot = as.data.frame(df_all_spreads['2016-07-01/',c('ma_series_spread','percentile_spread')])

df_plot$dates = as.Date(rownames(df_plot)) #need for ggpolt
df_plot = melt(df_plot,id.vars = 'dates',measure.vars = c('spread','percentile_spread'))
head(df_plot)
#p1 = ggplot(df_plot, aes(dates, spread)) + geom_line(colour='red') #+ scale_x_date(format = "%Y-%m-%d") + xlab("")
#p1
p2 = ggplot(df_plot, aes(dates,value,colour=variable)) + facet_grid(variable~.,scales="free") + geom_line()
p2

head(df_plot)

#Compare to those in daily run

pathDropbox = "/home/rstudio/"

path_to_crosscomo_spread = paste0(pathDropbox,
                                  "Dropbox/Libs/trading_analytics/cross_commo_seasonality/2017_02_24/",
                                  "seasonality_2017_02_24.RData")

load(path_to_crosscomo_spread)

names(seasonality_res)

wcn_report = seasonality_res[[90]]
names(wcn_report)

library(dplyr)
library(tibble)

# -----------   Calcing percentage spread for report

library(dplyr)
library(tibble)
pathDropbox = "/home/rstudio/Dropbox"


#get latest file
date = "2017_02_24"
path_to_crosscomo_spread = paste0(pathDropbox,
                                  "/Libs/trading_analytics/cross_commo_seasonality/2017_02_24/",
                                  "seasonality_2017_02_24.RData")

load(path_to_crosscomo_spread)




names(seasonality_res)
wcn_report = seasonality_res[[90]]

rolling_width = 20
date_initialise_start = as.Date("2001-06-01")
date_pc_window_start = as.Date("2001-07-01")
sp
df_spread_and_pc <-  spread_report %>% 
  as.data.frame() %>%
  rownames_to_column('dates') %>% 
  mutate(dates = as.Date(dates,format="%Y-%m-%d")) %>%
  filter(dates > date_initialise_start) %>%
  arrange(dates)%>%
  mutate(sma = rollapply(data=spread,
                         width = rolling_width, 
                         align='right',
                         FUN = mean,
                         fill = NA, 
                         na.rm = F)) %>%
  filter(dates > date_pc_window_start) %>%
  mutate(diff_to_sma = spread - sma) %>%
  mutate(pc_rank = 100*dense_rank(diff_to_sma)/length(diff_to_sma)) 


pc_rank = df_spread_and_pc %>% 
  tail(1) %>%
  select(pc_rank)



# ------------------- Comparing mikes method to mine

spread_report <- wcn_report$spread %>% 
  as.data.frame() %>%
  rownames_to_column('dates') %>% 
  mutate(dates = as.Date(dates,format="%Y-%m-%d"), variable = "spread_report") %>%
  select(dates, values=WCN1, variable)

df_plot3 %>% 
  filter(variable %in% c("spread","spread_report","sma","ma_series_spread")) %>%
  spread(variable,values)%>% 
  filter(dates > as.Date("2001-07-01")) %>%
  mutate(spread_report = na.locf(spread_report)) %>%
  rowwise() %>%
  mutate(spreadequal = spread == spread_report) %>%
  as.data.frame() %>%
  mutate(sma_report = rollapply(data=spread_report,
                                width = 28, 
                                align='right',
                                FUN = mean,
                                fill = NA, 
                                na.rm = F)) %>%
  mutate(sma_diffs = sma - sma_report, series_diffs = spread - spread_report) %>%
  mutate(spread2ma_report = spread_report - sma_report) %>%
  mutate(diff_ma_series_spread = ma_series_spread - spread2ma_report) %>%
  select(dates, 
         series_diffs,
         sma,
         sma_report,
         sma_diffs,
         spread,
         spread_report)%>%
  # select(dates, 
  #        ma_series_spread,
  #        spread2ma_report, 
  #        spread,spread_report, 
  #        diff_ma_series_spread) %>%
  gather(variable, values, -dates) %>%
  ggplot +
  aes(dates, values, colour=variable) +
  facet_grid(variable~.,scales="free") +
  geom_line()



max_date_collected = max(spread_report$dates)

#max date in contructed
df_all_spreads %>% index %>% as.Date %>% max


df_plot2 %>%
  ggplot +
  aes(dates, values, colour=variable) +
  facet_grid(variable~.,scales="free") +
  geom_line()


df_plot3 <- df_plot2 %>% 
  bind_rows(spread_report) %>%
  filter(dates < max(spread_report$dates))


#plot moving averages and differences

df_plot3 %>% 
  filter(variable %in% c("spread","spread_report","sma","ma_series_spread")) %>%
  spread(variable,values)%>% 
  filter(dates > as.Date("2001-07-01")) %>%
  mutate(spread_report = na.locf(spread_report)) %>%
  rowwise() %>%
  mutate(spreadequal = spread == spread_report) %>%
  as.data.frame() %>%
  mutate(sma_report = rollapply(data=spread_report,
                                width = 28, 
                                align='right',
                                FUN = mean,
                                fill = NA, 
                                na.rm = F)) %>%
  mutate(sma_diffs = sma - sma_report, series_diffs = spread - spread_report) %>%
  mutate(spread2ma_report = spread_report - sma_report) %>%
  mutate(diff_ma_series_spread = ma_series_spread - spread2ma_report) %>%
  select(dates, 
         series_diffs,
         sma,
         sma_report,
         sma_diffs,
         spread,
         spread_report)%>%
  # select(dates, 
  #        ma_series_spread,
  #        spread2ma_report, 
  #        spread,spread_report, 
  #        diff_ma_series_spread) %>%
  gather(variable, values, -dates) %>%
  ggplot +
  aes(dates, values, colour=variable) +
  facet_grid(variable~.,scales="free") +
  geom_line()

#What we can see is that there are spikes between the different series
#this just corresponds to that mikes series typicall switch to the next years contract a little earlier than bbg
# so if there's a big difference then we see that.


df_perc <-  df_plot3 %>% 
  filter(variable %in% c("spread","spread_report","sma","ma_series_spread")) %>%
  spread(variable,values)%>% 
  filter(dates > as.Date("2001-05-01")) %>%
  mutate(spread_report = na.locf(spread_report)) %>%
  rowwise() %>%
  mutate(spreadequal = spread == spread_report) %>%
  as.data.frame() %>%
  mutate(sma_report = rollapply(data=spread_report,
                                width = 28, 
                                align='right',
                                FUN = mean,
                                fill = NA, 
                                na.rm = F)) %>%
  filter(dates > as.Date("2001-07-01")) %>%
  mutate(sma_diffs = sma - sma_report, series_diffs = spread - spread_report) %>%
  mutate(spread2ma_report = spread_report - sma_report) %>%
  mutate(diff_ma_series_spread = ma_series_spread - spread2ma_report) %>%
  select(dates, 
         computeddata = ma_series_spread,
         reportdata = spread2ma_report)%>%
  gather(source, futures_spreads, -dates) %>%
  group_by(source) %>%
  mutate(pc_rank = 100*dense_rank(futures_spreads)/length(futures_spreads)) %>%
  select(dates,source,pc_rank) %>%
  as.data.frame %>%
  #spread(variable,values) %>%
  spread(source,pc_rank) %>%
  mutate(perc_diff = computeddata - reportdata) 

df_perc %>%
  ggplot +
  aes(dates, perc_diff) + #, colour=source) +
  #facet_grid(source~.,scales="free") +
  geom_line()

library(scales)  

df_perc %>% tail(30)

df_perc %>%
  ggplot +
  aes(x=perc_diff) +
  #aes(x=perc_diff,y =(..count../sum(..count..))) + #, colour=source) +
  #scale_y_continuous(labels = percent_format() )+
  #facet_grid(source~.,scales="free") +
  geom_histogram(binwidth = 20) +
  scale_y_continuous(breaks=pretty_breaks(n=50))




rank_spread = rank(as.vector(df_all_spreads$ma_series_spread),na.last=NA)
nrank = length(rank_spread)
percentile_spread = rank_spread/max(rank_spread)
length(percentile_spread)
df_all_spreads$rank = xts(rank_spread,order.by = index(df_all_spreads)[1:nrank])
df_all_spreads$percentile_spread = xts(percentile_spread,order.by = index(df_all_spreads)[1:nrank])



#Tickers are C,W
#months are H,K, N,  U, Z = MAR, MAY, JUL, SEP, DEC

#we need burn in period for sma, so need to get underlying contracts across years
#so we pull all those tickers, months, period years
#let's do in one query

#sql will be, construct filter, 
#get_id for contract_db, 
#ticker+month+year
#for those id, get date range
#select id, 


#put in list


#calc sma n weeks
# note you need n weeks prior to start of included period for the current contract
# e.g if you are doing a 4-week sma and the new contract rolls in at 1 july 20xx, then you need the data for 4 weeks prior to 1 july 20xx


#calc spread of spread_series to sma_series and then rank over period

#query all tickers

tickers = c("C","W")
month_subscript = c("H","K","N","U","Z")




