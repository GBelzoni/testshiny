#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#
library(stringr)
library(dplyr)
library(RMySQL)
library(shiny)
library(ggplot2)
script.dir = normalizePath(".")
#source(script.dir %>% paste0("/PatrickShinyTest/TestShiny/appObjects.R"))

#source(script.dir %>% paste0("/PatrickShinyTest/TestShiny/config.R")





# Define UI for application that draws a histogram
ui <- fluidPage(
   
  
  
   # Application title
   titlePanel("TestApp"),
   
   # Sidebar with a slider input for number of bins 
   sidebarLayout(
      sidebarPanel(
         uiOutput("firstTickerSelect"),
         uiOutput("secondTickerSelect")
      ),
      # Show a plot of the generated distribution
      mainPanel(
        plotOutput("plot"),
        dataTableOutput("datatable"),
        downloadButton('downloadData', 'Download')
      )
   )
)

# Define server logic required to draw a histogram
server <- function(input, output) {
  

  # source('../matcham/datalayer/getUnderlyingSeriesMatchingTickerYears.R')
  # source("../matcham/PatrickShinyTest/TestShiny/config.R", local = TRUE)
  # 
  source('getUnderlyingSeriesMatchingTickerYears.R')
  source("config.R", local = TRUE)
  
  
  
  
  #---- make ticker combos
  conn = dbConnect(MySQL(), user=user, password=password, dbname=dbname, host=host)
  reference_db = get_reference_db(conn)
  #conn %>% dbDisconnect
  
  tickersYY = c()
  for(i in 1:nrow(reference_db)){
    ticker = reference_db$ticker[i]
    months = reference_db$contract_month[i] %>% str_split(',') %>% unlist
    ticker_combos = months %>% paste0(ticker,.) %>% combn(2)
    tickersYY = c(tickersYY,ticker_combos)
  }
  
  tickersYY = unique(tickersYY)
  
  output$firstTickerSelect <-renderUI({
    selectInput("firstTickerSelect", "Select First Ticker", choices=tickersYY, selected = 'WN')
  })
  
  output$secondTickerSelect <-renderUI({
    selectInput("secondTickerSelect", "Select Second Ticker", choices=tickersYY, selected = 'CN')
  })
  
  output$text = renderText({input$firstTickerSelect})
  
  datatableReactive <- reactive({
    tickers_all = c(input$firstTickerSelect, input$secondTickerSelect)
    
    start_year = 2001
    ticker_weights = c(1,-1)
    
    conn = dbConnect(MySQL(), user=user, password=password, dbname=dbname, host=host)
    data = get_aligned_spread(tickers_all = tickers_all,start_year = start_year,ticker_weights = ticker_weights,conn = conn)
    #conn %>% dbDisconnect
    
    # --------------- Lookup underlying tickerrs and first notice periods for the contracts in the list
    # looks up main_schema.ticker_db for all tickerYY and notice periods matching those in the list
    
    rolling_width = 20 #This is 20day = 4weeks not including weekends
    df_futures_close_spreads <- data %>%
      select(date, close, ticker, start_date, spread_weight,charyear) %>%
      mutate(weighted_close = close*spread_weight) %>%
      #Two lines below group into date, and weighted closes, then sum to get weighted spread
      group_by(date,charyear) %>%
      summarize(futures_spread = sum(weighted_close),start_date = max(start_date)) %>%
      #Now we create rolling ma by grouping into charyear which has each spreads initialisation and report dates
      #then make sure we order by date
      #then use zoo packages rollapply function to make sma
      group_by(charyear)%>% 
      arrange(date) %>%
      mutate(sma = coredata(rollapply(
        data= zoo(futures_spread,date), 
        width = rolling_width, 
        align='right',
        FUN = mean,
        fill = NA, 
        na.rm = T))) %>% # TOD we want to be careful about how we pad, fill na et
      #Now we filter out initialisation period for ma to avoid double counts of dates
      filter(date> start_date) %>%
      #then calc the spread between the moving average and it's the futures_spread series
      mutate(sma_future_spread = futures_spread - sma) %>%
      #Now calc the latest values rand
      mutate(pc_rank = 100*dense_rank(sma_future_spread)/length(sma_future_spread)) %>%
      ungroup %>%
      select( date, futures_spread, sma, sma_future_spread, pc_rank) %>%
      arrange(date)
    
    df_futures_close_spreads
    
  })
  
  output$datatable <- renderDataTable( { 
    
    datatableReactive()
    
    })
  
  
  output$downloadData <- downloadHandler(
    filename = function() { paste('spreadData', '.csv', sep='') },
    content = function(file) {
      write.csv(datatableReactive(), file, row.names = FALSE)
    }
  )

  #plot Handler
  output$plot <- renderPlot({

      plot <-
        datatableReactive() %>%
        mutate(date = as.Date(date)) %>%
        filter(date > '2016-07-01') %>%
        select(date,futures_spread, pc_rank) %>%
        gather(variable, values, -date) %>%
        ggplot +
        aes(date, values, colour=variable) +
        facet_grid(variable~.,scales="free") +
        geom_line()

      plot

   })
}

# Run the application 
shinyApp(ui = ui, server = server)

